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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
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

	private static long reconnectCount = 0;
	private static long threadCount = 0;

	private long warmupReads = 0;
	private long interval = -1l;
	private TimeUnit intervalUnit = TimeUnit.SECONDS;
	private long previousStart = 0;

	private ThreadPoolExecutor mbeanExecutor;
	private ThreadPoolExecutor reconnectExecutor;

	private List<Attribute> attributes = Collections.synchronizedList(new ArrayList<Attribute>());
	private HashSet<Connection> connections = new HashSet<Connection>();

	private Set<AttributePermutation> collectablePermutations =
			Collections.synchronizedSet(new HashSet<AttributePermutation>());

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
	 * <li>The Hostname is auto-detected from the ServiceURL unless specified as the "string" portion of the Connection
	 * definition. The "host" property is still supported for backwards-compatibility.</li>
	 * <li>The Value block's string value is added to the "Attribute" list. The "Attribute" property is still supported for
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
	 *       #Attribute "Usage"
	 *       #Attribute min
	 *       #Attribute max
	 *       #Attribute full
	 *     </Value>
	 *   </MBean/Bean>
	 *
	 *   <Connection "host">
	 *       ServiceURL "service:jmx:rmi:///jndi/rmi://localhost:8675/jmxrmi"
	 *       Collect "alias"
	 *       User|Username "admin"
	 *       Password "foobar"
	 *       InstancePrefix "foo-"
	 *   </Connection>
	 * </Plugin>
	 * }
	 * </pre>
	 */
	public int config(OConfigItem ci) {
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
							}
						}
						Collectd.logDebug("FastJMX Plugin: Adding " + beanAlias + " for " + matchObjectName);

						// Adds the attribute definition.
						beanAttributes.add(new Attribute(valueAttributes, valueDs, valueInstancePrefix, valueInstanceFrom,
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
					}
				}

				if (serviceURL != null) {
					if (!beanAliases.isEmpty()) {
						// Try to parse the host name from the serviceURL, if none was defined.
						if (hostName == null) {
							hostName = serviceURL.getHost();

							// If that didn't work, try shortening the URL to something more manageable.
							if ((hostName == null || "".equals(hostName)) && rawUrl.lastIndexOf("://") > rawUrl.lastIndexOf(":///")) {
								try {
									hostName = new JMXServiceURL(rawUrl.substring(0, rawUrl.indexOf(":///"))
											                             + rawUrl.substring(rawUrl.lastIndexOf("://"))).getHost();
								} catch (MalformedURLException me) {
									hostName = Collectd.getHostname();
									Collectd.logWarning("Unable to parse hostname from JMX service URL: [" + rawUrl + "]. Falling back to hostname reported by this collectd instance: [" + hostName + "]");
								}
							}
						}

						// Now create the Connection and put it into our hashmap.
						connections.add(new Connection(this, rawUrl, hostName, serviceURL, username, password, connectionInstancePrefix, beanAliases));
					} else {
						Collectd.logError("Excluding Connection for : " + serviceURL.toString() + ". No beans to collect.");
					}
				} else {
					Collectd.logWarning("Excluding host definition no ServiceURL defined.");
				}
			} else if ("interval".equalsIgnoreCase(pluginChild.getKey())) {
				try {
					interval = pluginChild.getValues().get(0).getNumber().longValue();
				} catch (Exception ex) {
					ex.printStackTrace();
					Collectd.logWarning("FastJMX plugin: " + "Could not parse Interval value. Defaulting to auto-detect.");
				}
			} else if ("intervalunit".equalsIgnoreCase(pluginChild.getKey())) {
				try {
					intervalUnit = TimeUnit.valueOf(pluginChild.getValues().get(0).getString());
				} catch (Exception ex) {
					Collectd.logWarning("FastJMX plugin: " + "Could not parse Interval Unit. Defaulting to SECONDS");
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
		for (Connection connectionEntry : connections) {
			Collectd.logInfo("FastJMX plugin: Initiating Connection to: " + connectionEntry.rawUrl);
			connectionEntry.connect();
		}

		ThreadGroup fastJMXThreads = new ThreadGroup("FastJMX");
		fastJMXThreads.setDaemon(true);
		fastJMXThreads.setMaxPriority(Thread.MAX_PRIORITY);


		final ThreadGroup mbeanReaders = new ThreadGroup(fastJMXThreads, "MbeanReaders");
		mbeanExecutor = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 2,
				                                      Math.max(Runtime.getRuntime().availableProcessors() * 2, collectablePermutations.size() / 2),
				                                      interval > 0 ? interval : 30, intervalUnit,
				                                      new LinkedBlockingQueue<Runnable>(),
				                                      new ThreadFactory() {
					                                      public Thread newThread(Runnable r) {
						                                      Thread t =
								                                      new Thread(mbeanReaders, r, "mbean-reader-" + threadCount++);
						                                      t.setDaemon(mbeanReaders.isDaemon());
						                                      t.setPriority(Thread.MAX_PRIORITY - 2);
						                                      return t;
					                                      }
				                                      });
		mbeanExecutor.allowCoreThreadTimeOut(true);
		mbeanExecutor.prestartAllCoreThreads();

		final ThreadGroup reconnectors = new ThreadGroup(fastJMXThreads, "Reconnectors");

		reconnectExecutor =
				new ThreadPoolExecutor(0, Runtime.getRuntime().availableProcessors(), 30, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(connections.size()), new ThreadFactory() {
					public Thread newThread(Runnable r) {
						Thread t = new Thread(reconnectors, r, "reconnector-" + reconnectCount++);
						t.setDaemon(reconnectors.isDaemon());
						t.setPriority(Thread.MIN_PRIORITY);
						return t;
					}
				});
		reconnectExecutor.allowCoreThreadTimeOut(true);

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
		long start = System.nanoTime();

		List<Future<AttributePermutation>> results = new ArrayList<Future<AttributePermutation>>();
		synchronized (collectablePermutations) {
			try {
				if (interval < 0) {
					warmupReads++;
					mbeanExecutor.setCorePoolSize(1);
					mbeanExecutor.setMaximumPoolSize(1);

					if (warmupReads > 3) {
						interval = start - previousStart;
						intervalUnit = TimeUnit.NANOSECONDS;
						Collectd.logInfo("FastJMX plugin: Setting auto-detected interval to " + interval + " " + intervalUnit.name());
						mbeanExecutor.setCorePoolSize(Runtime.getRuntime().availableProcessors() * 2);
						mbeanExecutor.setMaximumPoolSize(Math.max(Runtime.getRuntime().availableProcessors() * 2, collectablePermutations.size() / 2));
						Collectd.logInfo("FastJMX plugin: Setting thread pool size " + mbeanExecutor.getCorePoolSize() + "~" + mbeanExecutor.getMaximumPoolSize());
					}
				}

				// Until we've been able to detect a
				if (interval > 0) {
					results = mbeanExecutor.invokeAll(collectablePermutations, interval, intervalUnit);
				} else {
					results = mbeanExecutor.invokeAll(collectablePermutations);
				}
			} catch (InterruptedException ie) {
				Collectd.logNotice("FastJMX plugin: Interrupted during read() cycle.");
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
		long duration = System.nanoTime() - start;
		Collectd.logInfo("FastJMX plugin: failed/cancelled/successful:" + failed + "/" + cancelled + "/" + success + " in " + TimeUnit.MILLISECONDS.convert(duration, TimeUnit.NANOSECONDS) + "ms");

		previousStart = start;
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
	 * Handles JMXConnectionNotifications from Connection objects.
	 *
	 * @param notification
	 * @param handback     The Connection
	 */
	public void handleNotification(Notification notification, Object handback) {
		if (notification instanceof JMXConnectionNotification) {
			final Connection connection = (Connection) handback;

			if (notification.getType().equals(JMXConnectionNotification.OPENED)) {
				Collectd.logDebug("FastJMX plugin: Creating collectablePermutations for " + connection.rawUrl);
				// Create the AttributePermutation objects appropriate for this Connection.
				synchronized (attributes) {
					for (Attribute attrib : attributes) {
						// If the host is supposed to collect this attribute, look for matching objectNames on the host.
						if (connection.beanAliases.contains(attrib.beanAlias)) {
							Collectd.logDebug("FastJMX plugin: Looking for " + attrib.findName + " @ " + connection.rawUrl);
							try {
								Set<ObjectName> instances =
										connection.getServerConnection().queryNames(attrib.findName, null);
								Collectd.logDebug("FastJMX plugin: Found " + instances.size() + " instances of " + attrib.findName + " @ " + connection.rawUrl);
								collectablePermutations.addAll(AttributePermutation.create(instances.toArray(new ObjectName[instances.size()]), connection, attrib));
							} catch (IOException ioe) {
								Collectd.logError("FastJMX plugin: Exception! " + ioe);
								connection.close();
							}
						}
					}
				}
			} else if (notification.getType().equals(JMXConnectionNotification.CLOSED) ||
					           notification.getType().equals(JMXConnectionNotification.FAILED)) {
				Collectd.logDebug("FastJMX plugin: Removing collectable permutations for " + connection.rawUrl);
				// Remove the AttributePermutation objects appropriate for this Connection.
				synchronized (collectablePermutations) {
					ArrayList<AttributePermutation> toRemove = new ArrayList<AttributePermutation>();
					for (AttributePermutation permutation : collectablePermutations) {
						if (permutation.getConnection().equals(connection)) {
							toRemove.add(permutation);
						}
					}
					collectablePermutations.removeAll(toRemove);
				}

				reconnectExecutor.execute(new Runnable() {
					public void run() {
						connection.connect();
					}
				});
			}
		} else if (notification instanceof MBeanServerNotification) {
			Connection connection = (Connection) handback;
			MBeanServerNotification serverNotification = (MBeanServerNotification) notification;

			if (notification.getType().equals(MBeanServerNotification.REGISTRATION_NOTIFICATION)) {
				// Check the objectName against attributes, and if matched, check if this connection should have it.
				synchronized (attributes) {
					for (Attribute attrib : attributes) {
						if (attrib.findName.apply(serverNotification.getMBeanName()) && connection.beanAliases.contains(attrib.beanAlias)) {
							collectablePermutations.addAll(AttributePermutation.create(new ObjectName[]{serverNotification.getMBeanName()}, connection, attrib));
						}
					}
				}
			} else if (notification.getType().equals(MBeanServerNotification.UNREGISTRATION_NOTIFICATION)) {
				// Any collectablePermutations for this connection and object name are removed.
				synchronized (collectablePermutations) {
					ArrayList<AttributePermutation> toRemove = new ArrayList<AttributePermutation>();
					for (AttributePermutation permutation : collectablePermutations) {
						if (permutation.getConnection().equals(connection) && permutation.getObjectName().equals(serverNotification.getMBeanName())) {
							toRemove.add(permutation);
						}
					}
					collectablePermutations.removeAll(toRemove);
				}
			}
		}
	}

	/**
	 * Gets the first value (if it exists) from the OConfigItem as a String
	 *
	 * @param ci
	 * @return The string, or <code>null</code> if no string is found.
	 */
	private String getConfigString(OConfigItem ci) {
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
	private Boolean getConfigBoolean(OConfigItem ci) {
		List<OConfigValue> values;
		OConfigValue v;
		Boolean b;

		values = ci.getValues();
		if (values.size() != 1) {
			return null;
		}

		v = values.get(0);
		if (v.getType() != OConfigValue.OCONFIG_TYPE_BOOLEAN) {
			return null;
		}

		return (new Boolean(v.getBoolean()));
	}
}