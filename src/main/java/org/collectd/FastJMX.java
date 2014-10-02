package org.collectd;

import org.apache.commons.math3.analysis.differentiation.DerivativeStructure;
import org.apache.commons.math3.analysis.interpolation.NevilleInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialFunctionLagrangeForm;
import org.apache.commons.math3.optim.MaxEval;
import org.apache.commons.math3.optim.MaxIter;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;
import org.apache.commons.math3.optim.univariate.BrentOptimizer;
import org.apache.commons.math3.optim.univariate.SearchInterval;
import org.apache.commons.math3.optim.univariate.UnivariateObjectiveFunction;
import org.apache.commons.math3.optim.univariate.UnivariatePointValuePair;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.collectd.api.Collectd;
import org.collectd.api.CollectdConfigInterface;
import org.collectd.api.CollectdInitInterface;
import org.collectd.api.CollectdReadInterface;
import org.collectd.api.CollectdShutdownInterface;
import org.collectd.api.DataSet;
import org.collectd.api.OConfigItem;
import org.collectd.api.OConfigValue;

import javax.management.MBeanServerNotification;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectionNotification;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * FastJMX is a Collectd plugin that allows for lower latency read cycles on remote JMX hosts.
 * It does so by maintaining a threadpool which is used to read attribute values from remote hosts during a read
 * cycle.
 * <p/>
 * A future enhancement that I'm looking into is the addition of an agent library that when included in the remote hosts
 * configuration would allow notification-based async push of value changes which would be aggregated and stored into
 * collectd during a read cycle.
 */
public class FastJMX implements CollectdConfigInterface, CollectdInitInterface, CollectdReadInterface, CollectdShutdownInterface, NotificationListener {

	private int MAXIMUM_THREADS = 512;

	private long reads = 0;
	private long interval = 0l;
	private long targetLatency = 0l;

	private int optimalSearch = 1;

	private TimeUnit intervalUnit = TimeUnit.MILLISECONDS;
	private Ring<History> histogram = new Ring<History>(15);

	private static ThreadGroup fastJMXThreads = new ThreadGroup("FastJMX");
	private static ThreadGroup mbeanReaders = new ThreadGroup(fastJMXThreads, "MbeanReaders");
	private static ThreadPoolExecutor mbeanExecutor =
			new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new FastJMXThreadFactory());

	private static List<Attribute> attributes = new ArrayList<Attribute>();
	private static HashSet<Connection> connections = new HashSet<Connection>();

	private static List<AttributePermutation> collectablePermutations =
			Collections.synchronizedList(new ArrayList<AttributePermutation>(100));


	static {
		System.getProperties().put("sun.rmi.transport.tcp.connectTimeout", TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS));
		System.getProperties().put("sun.rmi.transport.tcp.handshakeTimeout", TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS));
	}

	public FastJMX() {
		Collectd.registerConfig("FastJMX", this);
		Collectd.registerInit("FastJMX", this);
		Collectd.registerRead("FastJMX", this);
		Collectd.registerShutdown("FastJMX", this);
	}

	/**
	 * Parses a config structure like the one below into the parts necessary to collect the information.
	 * <p/>
	 * Note the following changes from the GenericJMX Configuration:
	 * <ul>
	 * <li>Interval: when defined, values not collected within the interval will be discarded, the read attempts will
	 * be interrupted, and no values will be submitted to collectd. If you find that you're missing many values. By
	 * default, the interval is auto-detected by the FastJMX plugin based on the elapsed time between collectd invoking
	 * 'read()' on the FastJMX plugin. This is a tuning option.</li>
	 * <li>IntervalUnit: Changes the interval unit to the name of a Java TimeUnit. Defaults to SECONDS.</li>
	 * <li>"MBean", "MXBean", and "Bean" are now interchangeable. This plugin also works with MXBeans.</li>
	 * <li>The Hostname is auto-detected from the ServiceURL unless specified as the "string" portion of the org.collectd.Connection
	 * definition. The "host" property is still supported for backwards-compatibility.</li>
	 * <li>The Value block's string value is added to the "org.collectd.Attribute" list. The "org.collectd.Attribute" property is still supported for
	 * backards-compatibility.</li>
	 * <li>"table" and "composite" are aliases for each other.</li>
	 * <li>"user" and "username" are aliases for each other.</li>
	 * </ul>
	 * <p/>
	 * <pre>
	 * {@Code
	 * <Plugin "FastJMX">
	 *   Interval 3
	 *   IntervalUnit SECONDS
	 *
	 *   <MBean/MXBean/Bean "alias">
	 *     ObjectName "java.lang:type=MemoryPool,*"
	 *     InstancePrefix "memory_pool-"
	 *     InstanceFrom "name"
	 *
	 *     <Value "Usage">
	 *       Type "gauge"
	 *       #InstancePrefix "value-"
	 *       #InstanceFrom "name"
	 *       Table|Composite true
	 *       #org.collectd.Attribute "Usage"
	 *       #org.collectd.Attribute min
	 *       #org.collectd.Attribute max
	 *       #org.collectd.Attribute full
	 *     </Value>
	 *   </MBean/Bean>
	 *
	 *   <org.collectd.Connection "host">
	 *       ServiceURL "service:jmx:rmi:///jndi/rmi://localhost:8675/jmxrmi"
	 *       Collect "alias"
	 *       User|Username "admin"
	 *       Password "foobar"
	 *       InstancePrefix "foo-"
	 *   </org.collectd.Connection>
	 * </Plugin>
	 * }
	 * </pre>
	 */
	public int config(final OConfigItem ci) {
		for (OConfigItem pluginChild : ci.getChildren()) {
			if ("mbean".equalsIgnoreCase(pluginChild.getKey()) || "mxbean".equalsIgnoreCase(pluginChild.getKey()) || "bean".equalsIgnoreCase(pluginChild.getKey())) {
				String beanAlias = getConfigString(pluginChild).toLowerCase();
				ObjectName matchObjectName = null;
				String beanInstancePrefix = null;
				List<String> beanInstanceFrom = new ArrayList<String>();

				for (OConfigItem beanChild : pluginChild.getChildren()) {
					if ("objectname".equalsIgnoreCase(beanChild.getKey())) {
						try {
							matchObjectName = new ObjectName(getConfigString(beanChild));
						} catch (MalformedObjectNameException ne) {
							matchObjectName = null;
						}
					} else if ("instanceprefix".equalsIgnoreCase(beanChild.getKey())) {
						beanInstancePrefix = getConfigString(beanChild);
					} else if ("instancefrom".equalsIgnoreCase(beanChild.getKey())) {
						beanInstanceFrom.add(getConfigString(beanChild));
					} else if ("value".equalsIgnoreCase(beanChild.getKey())) {
						List<String> valueAttributes = new ArrayList<String>();
						if (getConfigString(beanChild) != null) {
							valueAttributes.add(getConfigString(beanChild));
						}

						DataSet valueDs = null;
						String valueInstancePrefix = null;
						List<String> valueInstanceFrom = new ArrayList<String>();
						boolean composite = false;
						String pluginName = null;
						List<Attribute> beanAttributes = new ArrayList<Attribute>();

						for (OConfigItem valueChild : beanChild.getChildren()) {
							if ("attribute".equalsIgnoreCase(valueChild.getKey())) {
								valueAttributes.add(getConfigString(valueChild));
							} else if ("type".equalsIgnoreCase(valueChild.getKey())) {
								valueDs = Collectd.getDS(getConfigString(valueChild));
							} else if ("instanceprefix".equalsIgnoreCase(valueChild.getKey())) {
								valueInstancePrefix = getConfigString(valueChild);
							} else if ("instancefrom".equalsIgnoreCase(valueChild.getKey())) {
								valueInstanceFrom.add(getConfigString(valueChild));
							} else if ("table".equalsIgnoreCase(valueChild.getKey()) || "composite".equalsIgnoreCase(valueChild.getKey())) {
								composite = getConfigBoolean(valueChild);
							} else if ("pluginname".equalsIgnoreCase(valueChild.getKey())) {
								pluginName = getConfigString(valueChild);
							}
						}

						Collectd.logDebug("FastJMX plugin: Adding " + beanAlias + " for " + matchObjectName);

						// Adds the attribute definition.
						beanAttributes.add(new Attribute(valueAttributes, pluginName, valueDs, valueInstancePrefix, valueInstanceFrom,
								                                composite, beanAlias, matchObjectName,
								                                beanInstancePrefix, beanInstanceFrom));

						// Make sure the number of attributes matches the number of datasource values.
						if (valueDs.getDataSources().size() == valueAttributes.size()) {
							attributes.addAll(beanAttributes);
						} else {
							Collectd.logError("FastJMX plugin: The data set for bean '" + beanAlias + "' of type '"
									                  + valueDs.getType() + "' has " + valueDs.getDataSources().size()
									                  + " data sources, but there were " + valueAttributes.size()
									                  + " attributes configured. This bean will not be collected!");
						}
					}
				}
			} else if ("connection".equalsIgnoreCase(pluginChild.getKey())) {
				String hostName = getConfigString(pluginChild);
				boolean hostnamePort = false;
				String rawUrl = null;
				JMXServiceURL serviceURL = null;
				String username = null;
				String password = null;
				String connectionInstancePrefix = null;
				List<String> beanAliases = new ArrayList<String>();

				for (OConfigItem connectionChild : pluginChild.getChildren()) {
					if ("user".equalsIgnoreCase(connectionChild.getKey()) || "username".equalsIgnoreCase(connectionChild.getKey())) {
						username = getConfigString(connectionChild);
					} else if ("password".equalsIgnoreCase(connectionChild.getKey())) {
						password = getConfigString(connectionChild);
					} else if ("instanceprefix".equalsIgnoreCase(connectionChild.getKey())) {
						connectionInstancePrefix = getConfigString(connectionChild);
					} else if ("serviceurl".equalsIgnoreCase(connectionChild.getKey())) {
						rawUrl = getConfigString(connectionChild);
						try {
							serviceURL = new JMXServiceURL(rawUrl);
						} catch (MalformedURLException me) {
							Collectd.logError("FastJMX plugin: ServiceURL definition [" + getConfigString(connectionChild) + "] is invalid: " + me.getMessage());
							serviceURL = null;
						}
					} else if ("collect".equalsIgnoreCase(connectionChild.getKey())) {
						beanAliases.add(getConfigString(connectionChild).toLowerCase());
					} else if ("includeportinhostname".equalsIgnoreCase(connectionChild.getKey())) {
						hostnamePort = getConfigBoolean(connectionChild);
					}
				}

				if (serviceURL != null) {
					int port = serviceURL.getPort();
					if (!beanAliases.isEmpty()) {
						// Try to parse the host name from the serviceURL, if none was defined.
						if (hostName == null) {
							hostName = serviceURL.getHost();

							// If that didn't work, try shortening the URL to something more manageable.
							if ((hostName == null || "".equals(hostName)) && rawUrl.lastIndexOf("://") > rawUrl.lastIndexOf(":///")) {
								try {
									JMXServiceURL shortUrl =
											new JMXServiceURL(rawUrl.substring(0, rawUrl.indexOf(":///"))
													                  + rawUrl.substring(rawUrl.lastIndexOf("://")));
									hostName = shortUrl.getHost();
									port = shortUrl.getPort();
								} catch (MalformedURLException me) {
									hostName = Collectd.getHostname();
									Collectd.logWarning("Unable to parse hostname from JMX service URL: [" + rawUrl + "]. Falling back to hostname reported by this collectd instance: [" + hostName + "]");
								}
							}
						}

						if (hostnamePort) {
							hostName = hostName + "@" + port;
						}

						// Now create the org.collectd.Connection and put it into our hashmap.
						connections.add(new Connection(this, rawUrl, hostName, serviceURL, username, password, connectionInstancePrefix, beanAliases));
					} else {
						Collectd.logError("Excluding org.collectd.Connection for : " + serviceURL.toString() + ". No beans to collect.");
					}
				} else {
					Collectd.logWarning("Excluding host definition no ServiceURL defined.");
				}
			} else {
				Collectd.logError("FastJMX plugin: " + "Unknown config option: " + pluginChild.getKey());
			}
		}

		return 0;
	}


	/**
	 * During init() this plugin takes the time to do the 'long-running' JMX tasks which the GenericJMX plugin executes
	 * with each 'read()' cycle. This includes making JMX Connections to the remote hosts, building up the collectablePermutations
	 * for each attribute to be read, registering mbean and connection listeners to handle connection failures
	 * and registration changes on the remote servers, and finally initializing a threadpool to handle parallel,
	 * synchronous reading of JMX attributes.
	 *
	 * @return
	 */
	public int init() {
		mbeanExecutor.allowCoreThreadTimeOut(true);

		// Open connections.
		for (Connection connectionEntry : connections) {
			connectionEntry.connect();
		}

		return 0;
	}


	/**
	 * Queue a run of all the AttributePermutations we know of at the time read() is invoked, and wait up to the
	 * interval period for all collectablePermutations to complete.
	 * <p/>
	 * Any collectablePermutations which do not complete should be logged along with an interval warning.
	 * <p/>
	 * If 'interval' is not defined as a plugin parameter, we skip the first read() cycle to calculate the apparent
	 * interval Collectd is running with.
	 *
	 * @return
	 */
	public int read() {
		try {
			long start = System.nanoTime();

			// Rollover in case we're _really_ long running.
			if (reads++ == Long.MAX_VALUE) {
				reads = 1;
			}

			History previousCycle = histogram.peek();

			if (previousCycle != null) {
				interval = TimeUnit.MILLISECONDS.convert((start - previousCycle.started), TimeUnit.NANOSECONDS);
				intervalUnit = TimeUnit.MILLISECONDS;
				// Target latency default to half the interval, in nanoseconds.
				targetLatency = TimeUnit.NANOSECONDS.convert(interval / 2, TimeUnit.MILLISECONDS);

				// Adjust the keepalive time.
				if (interval * 2 > 0) {
					mbeanExecutor.setKeepAliveTime(interval * 2, intervalUnit);
				}
			}


			List<Future<AttributePermutation>> results = new ArrayList<Future<AttributePermutation>>(0);
			synchronized (collectablePermutations) {
				if (!collectablePermutations.isEmpty()) {
					Collections.sort(collectablePermutations);

					if (optimalSearch != 0) {
						int optimalSize = calculateOptimialSize();

						Collectd.logDebug("FastJMX Plugin: Optimal Pool size: " + (optimalSearch == 0 ? "FOUND" : "NOT FOUND") + " Setting Pool Size: " + optimalSize);
						mbeanExecutor.setCorePoolSize(optimalSize);
						mbeanExecutor.setMaximumPoolSize(optimalSize);
					}

					try {
						if (interval > 0) {
							results =
									mbeanExecutor.invokeAll(collectablePermutations, TimeUnit.MILLISECONDS.convert(interval, intervalUnit) - 10, TimeUnit.MILLISECONDS);
						} else {
							results = mbeanExecutor.invokeAll(collectablePermutations);
						}
					} catch (InterruptedException ie) {
						Collectd.logNotice("FastJMX plugin: Interrupted during read() cycle.");
					}
				}
			}

			int failed = 0;
			int cancelled = 0;
			int success = 0;
			for (Future<AttributePermutation> result : results) {
				try {
					Collectd.logDebug("FastJMX plugin: Read " + result.get().getObjectName() + " @ " + result.get().getConnection().rawUrl + " : " + result.get().getLastRunDuration());
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

			History completedCycle = new History(failed, cancelled, success, mbeanExecutor.getCorePoolSize(), start, System.nanoTime());

			// If the number of cancellations changed or the target latency is exceeded, recalculate.
			if (completedCycle.cancelled > 0 || completedCycle.duration > targetLatency) {
				optimalSearch = 1;
			}

			if (completedCycle.total > 0) {
				histogram.push(completedCycle);
			}
			Collectd.logDebug("FastJMX plugin: History: " + completedCycle);
		} catch (Throwable t) {
			Collectd.logError("FastJMX plugin: Unexpected Throwable: " + t);
		}
		return 0;
	}

	// Remove any notification listeners... clean up our stuffs then stop.
	public int shutdown() {
		mbeanExecutor.shutdown();
		try {
			// Wait a while for existing tasks to terminate
			if (!mbeanExecutor.awaitTermination(interval, intervalUnit)) {
				mbeanExecutor.shutdownNow(); // Cancel currently executing tasks
				// Wait a while for tasks to respond to being cancelled
				if (!mbeanExecutor.awaitTermination(interval, intervalUnit)) {
					Collectd.logWarning("FastJMX plugin: " + "Pool did not terminate cleanly.");
				}
			}
		} catch (InterruptedException ie) {
			// (Re-)Cancel if current thread also interrupted
			mbeanExecutor.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}

		return 0;
	}

	/**
	 * Creates AttributePermutations for the given connection, querying the remote MBeanServer for ObjectNames matching
	 * defined collect attributes for the connection.
	 *
	 * @param connection The Connection to create permutations and start collecting.
	 */
	private void createPermutations(final Connection connection) {
		Collectd.logDebug("FastJMX plugin: Creating AttributePermutations for " + connection.rawUrl);
		// Create the org.collectd.AttributePermutation objects appropriate for this org.collectd.Connection.
		for (Attribute attrib : attributes) {
			// If the host is supposed to collect this attribute, look for matching objectNames on the host.
			if (connection.beanAliases.contains(attrib.beanAlias)) {
				Collectd.logDebug("FastJMX plugin: Looking for " + attrib.findName + " @ " + connection.rawUrl);
				try {
					Set<ObjectName> instances =
							connection.getServerConnection().queryNames(attrib.findName, null);
					Collectd.logDebug("FastJMX plugin: Found " + instances.size() + " instances of " + attrib.findName + " @ " + connection.rawUrl);
					collectablePermutations.addAll(AttributePermutation.create(instances.toArray(new ObjectName[instances.size()]), connection, attrib, interval, intervalUnit));
					optimalSearch = 1;
				} catch (IOException ioe) {
					Collectd.logError("FastJMX plugin: Failed to find " + attrib.findName + " @ " + connection.rawUrl + " Exception message: " + ioe.getMessage());
				}
			}
		}
	}

	/**
	 * Removes all AttributePermutations for the given collection from our list of things to collect in a read cycle.
	 *
	 * @param connection The Connection to no longer collect.
	 */
	private void removePermutations(final Connection connection) {
		Collectd.logDebug("FastJMX plugin: Removing AttributePermutations for " + connection.rawUrl);
		// Remove the org.collectd.AttributePermutation objects appropriate for this org.collectd.Connection.
		ArrayList<AttributePermutation> toRemove = new ArrayList<AttributePermutation>();
		synchronized (collectablePermutations) {
			for (AttributePermutation permutation : collectablePermutations) {
				if (permutation.getConnection().equals(connection)) {
					toRemove.add(permutation);
				}
			}
			collectablePermutations.removeAll(toRemove);
			optimalSearch = -1;
		}
	}

	/**
	 * Creates AttributePermutations matching the connection and objectName.
	 *
	 * @param connection The Connection.
	 * @param objectName The name of an MBean, which may or may not match the attributes we're supposed to collect for
	 *                   the connection.
	 */
	private void createPermutations(final Connection connection, final ObjectName objectName) {
		for (Attribute attribute : attributes) {
			// If the host is supposed to collect this attribute, and the objectName matches the attribute, add the permutation.
			if (connection.beanAliases.contains(attribute.beanAlias) && attribute.findName.apply(objectName)) {
				collectablePermutations.addAll(AttributePermutation.create(new ObjectName[]{objectName}, connection, attribute, interval, intervalUnit));
				optimalSearch = 1;
			}
		}
	}

	/**
	 * Removes AttributePermutations matching the connection and objectName.
	 *
	 * @param connection The connection.
	 * @param objectName The name of an MBean which may or may not match AttributePermutations being collected.
	 */
	private void removePermutations(final Connection connection, final ObjectName objectName) {
		synchronized (collectablePermutations) {
			ArrayList<AttributePermutation> toRemove = new ArrayList<AttributePermutation>();
			for (AttributePermutation permutation : collectablePermutations) {
				if (permutation.getConnection().equals(connection) && permutation.getObjectName().equals(objectName)) {
					toRemove.add(permutation);
				}
			}
			collectablePermutations.removeAll(toRemove);
			optimalSearch = -1;
		}
	}

	/**
	 * Handles JMXConnectionNotifications from org.collectd.Connection objects.
	 *
	 * @param notification
	 * @param handback     The org.collectd.Connection
	 */
	public void handleNotification(final Notification notification, final Object handback) {
		if (notification instanceof JMXConnectionNotification) {
			final Connection connection = (Connection) handback;

			// If we get a connection opened, assume that the connection previously failed and we weren't notified.
			// This can happen if you're running collectd in a VM and you suspend the VM for a long period of time.
			if (notification.getType().equals(JMXConnectionNotification.OPENED)) {
				removePermutations(connection);
				createPermutations(connection);
			} else if (notification.getType().equals(JMXConnectionNotification.CLOSED) ||
					           notification.getType().equals(JMXConnectionNotification.FAILED)) {
				// Remove the permutations and reconnect.
				removePermutations(connection);
				connection.connect();
			}
		} else if (notification instanceof MBeanServerNotification) {
			Connection connection = (Connection) handback;
			MBeanServerNotification serverNotification = (MBeanServerNotification) notification;

			// A bean was added by a remote connection. Check it against the ObjectNames we're instrumenting.
			if (notification.getType().equals(MBeanServerNotification.REGISTRATION_NOTIFICATION)) {
				createPermutations(connection, serverNotification.getMBeanName());
			} else if (notification.getType().equals(MBeanServerNotification.UNREGISTRATION_NOTIFICATION)) {
				removePermutations(connection, serverNotification.getMBeanName());
			}
		}
	}

	/**
	 * Gets the first value (if it exists) from the OConfigItem as a String
	 *
	 * @param ci
	 * @return The string, or <code>null</code> if no string is found.
	 */
	private static String getConfigString(final OConfigItem ci) {
		List<OConfigValue> values;
		OConfigValue v;

		values = ci.getValues();
		if (values.size() != 1) {
			return null;
		}

		v = values.get(0);
		if (v.getType() != OConfigValue.OCONFIG_TYPE_STRING) {
			return null;
		}

		return (v.getString());
	}


	/**
	 * Gets the first value (if it exists) from the OConfigItem as a Boolean
	 *
	 * @param ci
	 * @return The Boolean value, or <code>null</code> if no boolean is found.
	 */
	private static Boolean getConfigBoolean(final OConfigItem ci) {
		List<OConfigValue> values;
		OConfigValue v;
		Boolean b;

		values = ci.getValues();
		if (values.size() != 1) {
			return false;
		}

		v = values.get(0);
		if (v.getType() != OConfigValue.OCONFIG_TYPE_BOOLEAN) {
			return false;
		}

		return (new Boolean(v.getBoolean()));
	}

	private class History {
		long started = 0;
		long ended = 0;
		long duration = 0;
		int poolSize = 0;
		int failed = 0;
		int cancelled = 0;
		int success = 0;
		int total = 0;

		public History(final int failed, final int cancelled, final int success, final int poolSize, final long started, final long ended) {
			this.failed = failed;
			this.cancelled = cancelled;
			this.success = success;
			this.poolSize = poolSize;
			this.started = started;
			this.ended = ended;
			this.total = failed + cancelled + success;
			this.duration = ended - started;
		}

		public int hashCode() {
			return new Double(((success + failed) / duration) * Math.pow(2, poolSize)).hashCode();
		}


		public String toString() {
			return "[failed: " + failed + ", canceled: " + cancelled + ", success: " + success + "] Took " + TimeUnit.MILLISECONDS.convert(duration, TimeUnit.NANOSECONDS) + "ms in a pool of " + poolSize + " threads.";
		}
	}


	private int estimateMinimumPoolSize() {
		if (collectablePermutations.isEmpty()) {
			return 1;
		}

		Collectd.logDebug("FastJMX Plugin: estimateMinimumPoolSize: targetLatency: " + targetLatency);

		int slowestReasonableIdx = -1;  // Items which cannot complete within the targetTime.
		AttributePermutation slowestReasonable = null;

		int queuableIdx = -1; // Items which can complete within the targetTime even after being queued.

		long estimatedEffort =
				0; // Best-guess Estimate on the remaining effort needed to execute items after the queueableIdx.
		long knownEfforts = 0;
		long unknownEffort = 0;

		for (int i = 0; i < collectablePermutations.size(); i++) {
			AttributePermutation ap = collectablePermutations.get(i);
			ap.setInterval(interval, intervalUnit);

			// Find the index of the first item that can run before targetTime.
			if (slowestReasonableIdx == -1 && ap.getLastRunDuration() <= targetLatency) {
				slowestReasonableIdx = i;
				slowestReasonable = ap;
			}

			// Find the index of the first item that could run on the same thread as the slowestReasonable, and still complete before targetTime.
			if (slowestReasonableIdx >= 0 && queuableIdx == -1 &&
					    ap.getLastRunDuration() + slowestReasonable.getLastRunDuration() <= targetLatency) {
				queuableIdx = i;
			}

			if (ap.getLastRunDuration() > 0) {
				estimatedEffort += ap.getLastRunDuration();
				knownEfforts++;
			} else {
				unknownEffort++;
			}
		}

		// Assuming we set threadsRequired = queueableIdx, how much effort will remain to be calculated?
		int requiredThreads = queuableIdx;

		Collectd.logDebug("FastJMX Plugin: estimateMinimumPoolSize: queuableIdx: " + queuableIdx + " unknownEffort: " + unknownEffort + " knownEfforts:" + knownEfforts + " estimatedEffort:" + estimatedEffort);

		// Get the mean time for the known runs from the last cycle, and use that to calculate the
		// times expected for the unknown efforts.
		estimatedEffort += (estimatedEffort / Math.max(knownEfforts, 1)) * unknownEffort;

		Collectd.logDebug("FastJMX Plugin: estimateMinimumPoolSize: estimateEffort accounting for unknowns @ average: " + estimatedEffort);

		// Now account for the threads.
		estimatedEffort = estimatedEffort - (requiredThreads * targetLatency);

		// How many threads should we have to finish by targetTime?
		requiredThreads += (int) Math.ceil((double) estimatedEffort / targetLatency);

		Collectd.logDebug("FastJMX plugin: estimateMinimumPoolSize: Calculated minimum pool size: " + requiredThreads);

		// Clamp to the global max threads
		requiredThreads = Math.min(requiredThreads, MAXIMUM_THREADS);
		Collectd.logDebug("FastJMX plugin: estimateMinimumPoolSize: After clamping to MAXIMUM_THREADS: " + requiredThreads);

		return requiredThreads;
	}


	private int calculateOptimialSize() {
		// Return the current number of threads if we have any issues calculating an optimal size.
		int optimalThreads = mbeanExecutor.getCorePoolSize();

		// Key = # of threads
		HashMap<Integer, List<History>> dataPoints = new HashMap<Integer, List<History>>(histogram.size());

		// Add Histogram entries that had the same amount of work as we currently have.
		for (int i = 0; i < histogram.size(); i++) {
			History candidate = histogram.get(i);
			if (candidate.total == collectablePermutations.size()) {
				List<History> pointList = dataPoints.get(candidate.poolSize);
				if (pointList == null) {
					pointList = new ArrayList<History>(histogram.size() / 2);
				}
				pointList.add(candidate);
				dataPoints.put(candidate.poolSize, pointList);
			}
		}


		if (!dataPoints.isEmpty()) {
			// Make sure we have enough data points.
			if (dataPoints.size() < 3) {
				Collectd.logDebug("FastJMX Plugin: Histogram does not contain enough different sized pools with comparable workloads to determine optimal thread count.");

				int min = Integer.MAX_VALUE;
				int max = 0;
				for (List<History> pointList : dataPoints.values()) {
					History point = pointList.get(0);
					min = Math.min(point.poolSize, min);
					max = Math.max(point.poolSize, max);
				}

				if (optimalSearch == -1 && min > Runtime.getRuntime().availableProcessors() + 1) {
					Collectd.logDebug("FastJMX Plugin: Forcing pool shrink to provide histogram data.");
					optimalThreads = min - Runtime.getRuntime().availableProcessors();
				} else {
					Collectd.logDebug("FastJMX Plugin: Forcing pool growth to provide histogram data.");
					optimalThreads = max + Runtime.getRuntime().availableProcessors();
				}
			} else {
				Collectd.logDebug("FastJMX Plugin: Found datapoints " + dataPoints.size() + " for current workload size.");
				// Take fastest run by average duration per item.

				for (Map.Entry<Integer, List<History>> entry : dataPoints.entrySet()) {
					Collections.sort(entry.getValue(), FASTEST_AVERAGE_DURATION_COMPARATOR);
				}

				List<Integer> poolSizes = new ArrayList<Integer>(dataPoints.keySet());
				Collections.sort(poolSizes);

				History[] points = new History[]{dataPoints.get(poolSizes.get(0)).get(0),
				                                 dataPoints.get(poolSizes.get(poolSizes.size() / 2)).get(0),
				                                 dataPoints.get(poolSizes.get(poolSizes.size() - 1)).get(0)};

				// Get the y values as the average duration per task
				double[] xvals = new double[points.length];
				double[] yvals = new double[points.length];

				StringBuilder log = new StringBuilder();
				for (int i = 0; i < points.length; i++) {
					xvals[i] = points[i].poolSize;
					yvals[i] = TimeUnit.MILLISECONDS.convert(points[i].duration, TimeUnit.NANOSECONDS);
					log.append("[").append(xvals[i]).append(", ").append(yvals[i]).append("] ");
				}
				Collectd.logDebug("FastJMX Plugin: condiering points: " + log);

				NevilleInterpolator interpolator = new NevilleInterpolator();
				PolynomialFunctionLagrangeForm function = interpolator.interpolate(xvals, yvals);
				Collectd.logDebug("FastJMX Plugin: Function degree: " + function.degree());

				BrentOptimizer optimizer = new BrentOptimizer(1e-10, 1e-14);
				try {
					int minimumThreads = estimateMinimumPoolSize();

					UnivariatePointValuePair optimalSize = optimizer.optimize(GoalType.MINIMIZE,
							                                               new SearchInterval(minimumThreads / 2, Math.min(collectablePermutations.size(), MAXIMUM_THREADS), minimumThreads / 2),
							                                               new UnivariateObjectiveFunction(function),
							                                               MaxEval.unlimited(), MaxIter.unlimited());

					Collectd.logDebug("FastJMX Plugin: Interpolated Polynomial predicts " + optimalSize.getPoint() + " as the most efficient # of threads.");
					optimalThreads = Math.max(minimumThreads, (int)Math.round(optimalSize.getPoint()));

					optimalSearch = 0;
				} catch (Exception ex) {
					Collectd.logWarning("FastJMX Plugin: Error calculating optimal thread count: " + ex);
				}
			}
		}

		optimalThreads = Math.min(optimalThreads, MAXIMUM_THREADS);
		Collectd.logDebug("FastJMX Plugin: Optimal Threads: " + optimalThreads);
		return optimalThreads;
	}

	private static final Comparator<History> FASTEST_AVERAGE_DURATION_COMPARATOR = new Comparator<History>() {
		public int compare(History o1, History o2) {
			return Long.compare(((o1.success + o1.failed) / o1.duration), ((o2.success + o2.failed) / o2.duration));
		}
	};

	private static class FastJMXThreadFactory implements ThreadFactory {
		private int threadCount = 0;

		public Thread newThread(Runnable r) {
			Thread t = new Thread(mbeanReaders, r, "mbean-reader-" + threadCount++);
			t.setDaemon(mbeanReaders.isDaemon());
			t.setPriority(Thread.MAX_PRIORITY - 2);
			return t;
		}
	}
}