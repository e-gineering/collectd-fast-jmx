package org.collectd;

import org.apache.commons.math3.analysis.MultivariateMatrixFunction;
import org.apache.commons.math3.analysis.MultivariateVectorFunction;
import org.apache.commons.math3.analysis.UnivariateFunction;
import org.apache.commons.math3.fitting.leastsquares.LeastSquaresBuilder;
import org.apache.commons.math3.fitting.leastsquares.LeastSquaresOptimizer;
import org.apache.commons.math3.fitting.leastsquares.LeastSquaresProblem;
import org.apache.commons.math3.fitting.leastsquares.LevenbergMarquardtOptimizer;
import org.apache.commons.math3.linear.DiagonalMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.optim.MaxEval;
import org.apache.commons.math3.optim.MaxIter;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;
import org.apache.commons.math3.optim.univariate.BrentOptimizer;
import org.apache.commons.math3.optim.univariate.SearchInterval;
import org.apache.commons.math3.optim.univariate.UnivariateObjectiveFunction;
import org.apache.commons.math3.optim.univariate.UnivariatePointValuePair;
import org.collectd.api.Collectd;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * A class that implements a ring buffer histogram of ReadCycleResults, encapsulates a ThreadPoolExecutor, and
 * uses least squares estimation to divine an optimum pool size.
 * <p/>
 * The basic premise is that given the command to invoke all AttributePermutations, the pool should be tuned so that the
 * next invocation of invokeAll() has a better chance of success prior to time-out than the current invocation had.
 * <p/>
 * The actual goal, of course, is to find the optimal number of threads to execute the given tasks with.
 */
public class SelfTuningCollectionExecutor {
	private static ThreadGroup fastJMXThreads = new ThreadGroup("FastJMX");
	private static ThreadGroup mbeanReaders = new ThreadGroup(fastJMXThreads, "MbeanReaders");
	private static long loaded = System.nanoTime();

	private static Comparator<Number> numberComparator = new Comparator<Number>() {
		public int compare(Number o1, Number o2) {
			return Double.compare(o1.doubleValue(), o2.doubleValue());
		}
	};

	private ReadCycleResult[] ring;
	private int index;

	private ThreadPoolExecutor threadPool;
	private int maxThreads;

	private int minIndependent;
	private boolean recalculateOptimum;

	private long interval = 0l;
	private TimeUnit intervalUnit = TimeUnit.MILLISECONDS;

	// Seed for a fibonacci sequence, which is used to manipulate pool sizing in search of data points for analysis.
	int fiba = 1;
	int fibb = 0;

	public SelfTuningCollectionExecutor(final int maximumThreads) {
		ring = new ReadCycleResult[45];
		minIndependent = 7;
		this.maxThreads = maximumThreads;
		threadPool = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS,
				                                   new LinkedBlockingQueue<Runnable>(), new FastJMXThreadFactory());
		threadPool.allowCoreThreadTimeOut(true);
		threadPool.setMaximumPoolSize(maximumThreads);

		this.clear();
	}

	/**
	 * Resets the histogram and pool sizes to their initial states.
	 */
	private void clear() {
		recalculateOptimum = true;
		resetFibonacci();

		for (int i = 0; i < ring.length; i++) {
			ring[i] = null;
		}
		index = 0;
	}

	/**
	 * Adds a new ReadCycleResult for consideration in future executions.
	 *
	 * @param cycle
	 */
	private void push(ReadCycleResult cycle) {
		if (cycle == null) {
			throw new IllegalArgumentException("Histogram does not support pushing 'null' values.");
		}

		Collectd.logDebug("FastJMX Plugin: " + cycle);
		if (cycle.getTotal() <= 0) {
			return;
		}

		if (cycle.triggerRecalculate(peek())) {
			recalculateOptimum = true;
		}

		// Modify the ring buffer.
		ring[index] = cycle;
		index = (index + 1) % ring.length;

		if (recalculateOptimum) {
			int threadCount = calculateOptimum();

			if (threadCount != threadPool.getCorePoolSize()) {
				Collectd.logInfo("FastJMX Plugin: Setting thread pool size: " + threadCount);
				threadPool.setCorePoolSize(threadCount);
				threadPool.setMaximumPoolSize(threadCount);
			}
		}
	}

	/**
	 * Shuts down the thread pool
	 */
	public void shutdown() {
		threadPool.shutdown();
		try {
			// Wait a while for existing tasks to terminate
			if (!threadPool.awaitTermination(interval, intervalUnit)) {
				threadPool.shutdownNow(); // Cancel currently executing tasks
				// Wait a while for tasks to respond to being cancelled
				if (!threadPool.awaitTermination(interval, intervalUnit)) {
					Collectd.logWarning("FastJMX plugin: ThreadPool did not terminate cleanly.");
				}
			}
		} catch (InterruptedException ie) {
			// (Re-)Cancel if current thread also interrupted
			threadPool.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Invokes the AttributePermutations with the thread pool executor and returns the results.
	 *
	 * @param tasks
	 * @return
	 * @throws InterruptedException
	 */
	public List<Future<AttributePermutation>> invokeAll(List<AttributePermutation> tasks) throws InterruptedException {
		long start = System.nanoTime();
		List<Future<AttributePermutation>> results;
		try {
			ReadCycleResult previousCycle = peek();
			interval = TimeUnit.NANOSECONDS.convert((start - (previousCycle != null ? previousCycle.getStarted() : loaded)), TimeUnit.NANOSECONDS);
			intervalUnit = TimeUnit.NANOSECONDS;
			if (interval * 2 > 0) {
				threadPool.setKeepAliveTime(interval * 2, intervalUnit);
			}

			for (int i = 0; i < tasks.size(); i++) {
				tasks.get(i).setInterval(interval, intervalUnit);
			}

			if (interval > 0) {
				results =
						threadPool.invokeAll(tasks, TimeUnit.MILLISECONDS.convert(interval, intervalUnit) - 10, TimeUnit.MILLISECONDS);
			} else {
				results = threadPool.invokeAll(tasks);
			}
		} finally {
			threadPool.purge();
		}

		int failed = 0;
		int cancelled = 0;
		int success = 0;
		for (Future<AttributePermutation> result : results) {
			try {
				Collectd.logDebug("FastJMX plugin: Read " + result.get().getObjectName() + " @ " + result.get().getConnection().getRawUrl() + " : " + result.get().getLastRunDuration());
				success++;
			} catch (ExecutionException ex) {
				failed++;
				Collectd.logError("FastJMX plugin: Failed " + ex.getCause());
			} catch (CancellationException ce) {
				cancelled++;
			} catch (InterruptedException ie) {
				Collectd.logDebug("FastJMX plugin: Interrupted while doing post-read interrogation.");
				break;
			}
		}

		push(new ReadCycleResult(failed, cancelled, success, start, System.nanoTime(), threadPool.getCorePoolSize(), interval));

		return results;
	}

	/**
	 * Looks at the last ReadCycleResult push()ed into the ring buffer
	 * @return
	 */
	private ReadCycleResult peek() {
		int pos = index;
		if (pos == 0) {
			pos = ring.length;
		}
		return ring[pos - 1];
	}

	private void resetFibonacci(){
		resetFibonacci(Runtime.getRuntime().availableProcessors());
	}

	private void resetFibonacci(int lowerBound) {
		int max;
		while (fiba + fibb > lowerBound) {
			max = fiba;

			fiba = fibb;
			fibb = max - fiba;
		}
		Collectd.logInfo("FastJMX Plugin: Fibonacci reset to : " + fiba + ":" + fibb);
	}

	/**
	 * Gets the next value in a fibonacci sequence....
	 * @return
	 */
	private int getNextFibonacci() {
		int current = fiba;
		int next = fiba + fibb;
		fibb = fiba;
		fiba = next;

		if (current > maxThreads) {
			resetFibonacci();
			current = getNextFibonacci();
		}

		Collectd.logInfo("FastJMX Plugin: fibonacci sequence generated: " + current);
		return current;
	}

	/**
	 * Easily the most complex part of this class --
	 *
	 * Using the ReadCycleResult objects in the ring buffer, organize the data into a hash map where the key is the
	 * pool size, and the value is the duration it took to complete.
	 *
	 * @return
	 */
	private int calculateOptimum() {
		int threadCount = threadPool.getCorePoolSize();
		if (recalculateOptimum) {
			HashMap<Integer, List<ReadCycleResult>> valueMap = new HashMap<Integer, List<ReadCycleResult>>();

			for (int i = 0; i < ring.length; i++) {
				if (ring[i] != null && ring[i].getPoolSize() > 0) {
					List<ReadCycleResult> depPoints = valueMap.get(ring[i].getPoolSize());
					if (depPoints == null) {
						depPoints = new ArrayList<ReadCycleResult>(5);
					}
					depPoints.add(ring[i]);
					valueMap.put(ring[i].getPoolSize(), depPoints);
				}
			}

			List<Integer> valueKeys = new ArrayList<Integer>(valueMap.keySet());
			Collections.sort(valueKeys, numberComparator);

			Collectd.logDebug("FastJMX Plugin: " + valueKeys.size() + " of " + minIndependent + " unique pool sizes for optimal projection");
			if (valueKeys.size() < minIndependent) {
				threadCount = getNextFibonacci();
			} else {
				// Compute the averages of the dependent variable and keep track of independent values and weights.
				double[] independent = new double[valueKeys.size()];
				double[] observation = new double[valueKeys.size()];
				double[] weights = new double[valueKeys.size()];
				for (int i = 0; i < valueKeys.size(); i++) {
					Number key = valueKeys.get(i);
					independent[i] = key.doubleValue();
					observation[i] = averageDuration(valueMap.get(key));
					weights[i] = weight(valueMap.get(key));
					Collectd.logDebug("FastJMX Plugin: Point: " + independent[i] + "," + observation[i] + " weight: " + weights[i]);
				}

				QuadraticProblem qp = new QuadraticProblem(independent, observation, weights);

				LeastSquaresProblem problem =
						new LeastSquaresBuilder().model(qp, qp.getMatrixFunc())
								.target(qp.calculateTarget())
								.start(new double[]{1, 1, 1})
								.maxEvaluations(100)
								.maxIterations(100)
								.weight(qp.getWeight())
								.build();

				LevenbergMarquardtOptimizer optimizer = new LevenbergMarquardtOptimizer();
				LeastSquaresOptimizer.Optimum optimum = optimizer.optimize(problem);
				QuadraticFunction qFunc = new QuadraticFunction(optimum.getPoint());

				BrentOptimizer bo = new BrentOptimizer(1e-10, 1e-14);
				UnivariatePointValuePair optimalMin = bo.optimize(GoalType.MINIMIZE,
						                                                 new SearchInterval(0, 512, 1),
						                                                 new UnivariateObjectiveFunction(qFunc),
						                                                 MaxEval.unlimited(), MaxIter.unlimited());

				Collectd.logDebug("FastJMX Plugin: Found minimum value: " + optimalMin.getValue() + " @ " + optimalMin.getPoint());
				threadCount = Math.max((int) Math.round(optimalMin.getPoint()), 1);

				// Reset the fibonacci sequence to a decent position.
				resetFibonacci(threadCount / 4);
			}

			// Clamp the new value to maxThreads....
			threadCount = Math.min(threadCount, maxThreads);
		}

		recalculateOptimum = false;
		return threadCount;
	}

	private Double averageDuration(List<ReadCycleResult> values) {
		double d = 0.0;
		for (int i = 0; i < values.size(); i++) {
			d += values.get(i).getDurationMs();
		}
		return d / values.size();
	}

	/**
	 * Calculate average jacobian weight for the list of read cycle results.
	 *
	 * @param values
	 * @return
	 */
	private Double weight(List<ReadCycleResult> values) {
		double d = 0.0;
		for (int i = 0; i < values.size(); i++) {
			d += values.get(i).getWeight();
		}
		return Math.max(d, 0) / values.size();
	}


	/**
	 * Implementation of a 2nd degree quadratic univariate function, ax^2 + bx + c
	 * <p/>
	 * Using the output of the QuadraticProblem (least squares solving) this can be used with a further
	 * optimizer to find the min value of the function. This min dependent variable value should coincide with the
	 * optimum dependent variable (# of threads in our case) to execute the workload in a timely manner.
	 */
	private class QuadraticFunction implements UnivariateFunction {
		double a;
		double b;
		double c;

		public QuadraticFunction(RealVector vector) {
			a = vector.getEntry(0);
			b = vector.getEntry(1);
			c = vector.getEntry(2);
		}

		public double value(double x) {
			return (a * Math.pow(x, 2)) + (b * x) + c;
		}
	}

	/**
	 * Creates a commons-math MultivariateVectorFunction that can feed a LeastSquaresProblem in order to project
	 * optimial thread pool size.
	 */
	private static class QuadraticProblem implements MultivariateVectorFunction {
		private double[] x;
		private double[] y;
		private double[] w;

		public QuadraticProblem(double[] independent, double[] observation, double[] weights) {
			if (independent.length != observation.length && weights.length != observation.length) {
				throw new IllegalArgumentException("Independent, observation, and weights must have the same number of elements.");
			}

			x = independent;
			y = observation;
			w = weights;
		}

		public double[] calculateTarget() {
			double[] target = new double[y.length];
			for (int i = 0; i < y.length; i++) {
				target[i] = y[i];
			}
			return target;
		}

		private double[][] jacobian(double[] variables) {
			double[][] jacobian = new double[x.length][3];
			for (int i = 0; i < jacobian.length; ++i) {
				jacobian[i][0] = x[i] * x[i];
				jacobian[i][1] = x[i];
				jacobian[i][2] = 1.0;
			}
			return jacobian;
		}

		public double[] value(double[] variables) {
			double[] values = new double[x.length];
			for (int i = 0; i < values.length; ++i) {
				values[i] = (variables[0] * x[i] + variables[1]) * x[i] + variables[2];
			}
			return values;
		}

		public MultivariateMatrixFunction getMatrixFunc() {
			return new MultivariateMatrixFunction() {
				public double[][] value(double[] point) {
					return jacobian(point);
				}
			};
		}

		public RealMatrix getWeight() {
			return new DiagonalMatrix(w);
		}
	}

	private class FastJMXThreadFactory implements ThreadFactory {
		private int threadCount = 0;

		public Thread newThread(Runnable r) {
			Thread t = new Thread(mbeanReaders, r, "mbean-reader-" + threadCount++);

			t.setDaemon(mbeanReaders.isDaemon());
			t.setPriority(Thread.MAX_PRIORITY - 2);
			return t;
		}
	}
}
