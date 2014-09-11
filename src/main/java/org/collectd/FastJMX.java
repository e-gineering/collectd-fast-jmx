package org.collectd;

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

	private long reads = 0;
	private long interval = 0l;
	private TimeUnit intervalUnit = TimeUnit.MILLISECONDS;
	private long previousStart = System.nanoTime();

	private ThreadPoolExecutor mbeanExecutor;

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
		// Configure the ThreadGroups and pools.
		ThreadGroup fastJMXThreads = new ThreadGroup("FastJMX");
		fastJMXThreads.setMaxPriority(Thread.MAX_PRIORITY);

		final ThreadGroup mbeanReaders = new ThreadGroup(fastJMXThreads, "MbeanReaders");
		mbeanExecutor = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS,
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
		mbeanExecutor.allowCoreThreadTimeOut(false);
		mbeanExecutor.prestartAllCoreThreads();

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
		long start = System.nanoTime();

		// Rollover in case we're _really_ long running.
		if (reads++ == Long.MAX_VALUE) {
			reads = 1;
		}

		// On the second cycle, and then every 10 read cycles, adjust the pool and interval.
		if (reads == 2 || reads % 10 == 0) {
			interval = TimeUnit.MILLISECONDS.convert(start - previousStart, TimeUnit.NANOSECONDS);
			intervalUnit = TimeUnit.MILLISECONDS;
			Collectd.logInfo("FastJMX plugin: Interval calculated at: " + interval + " " + intervalUnit);
			synchronized (collectablePermutations) {
				for (AttributePermutation collectable : collectablePermutations) {
					collectable.setInterval(interval, intervalUnit);
				}
			}

			mbeanExecutor.setCorePoolSize(Runtime.getRuntime().availableProcessors() * 2);
			mbeanExecutor.setMaximumPoolSize(Math.max(Runtime.getRuntime().availableProcessors() * 2, connections.size()));
			Collectd.logInfo("FastJMX plugin: Setting thread pool size " + mbeanExecutor.getCorePoolSize() + "~" + mbeanExecutor.getMaximumPoolSize());
		}

		// If we've detected an interval, invoke the attribute collectors with a maximum timeout of that interval period - 10 milliseconds.
		List<Future<AttributePermutation>> results = null;
		synchronized (collectablePermutations) {
			try {
				if (interval > 0) {
					results = mbeanExecutor.invokeAll(collectablePermutations, TimeUnit.MILLISECONDS.convert(interval, intervalUnit) - 10, TimeUnit.MILLISECONDS);
				} else {
					results = mbeanExecutor.invokeAll(collectablePermutations);
				}
			} catch (InterruptedException ie) {
				Collectd.logNotice("FastJMX plugin: Interrupted during read() cycle.");
			} finally {
				if (results == null) {
					results = new ArrayList<Future<AttributePermutation>>(0);
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
		long duration = System.nanoTime() - start;
		Collectd.logInfo("FastJMX plugin: [failed:" + failed + ", canceled:" + cancelled + ", successful:" + success + "] in " + TimeUnit.MILLISECONDS.convert(duration, TimeUnit.NANOSECONDS) + "ms with: " + mbeanExecutor.getPoolSize() + " threads");

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
	 * Creates AttributePermutations for the given connection, querying the remote MBeanServer for ObjectNames matching
	 * defined collect attributes for the connection.
	 *
	 * @param connection The Connection to create permutations and start collecting.
	 */
	private void createPermutations(final Connection connection) {
		Collectd.logDebug("FastJMX plugin: Creating AttributePermutations for " + connection.rawUrl);
		// Create the org.collectd.AttributePermutation objects appropriate for this org.collectd.Connection.
		synchronized (attributes) {
			for (Attribute attrib : attributes) {
				// If the host is supposed to collect this attribute, look for matching objectNames on the host.
				if (connection.beanAliases.contains(attrib.beanAlias)) {
					Collectd.logDebug("FastJMX plugin: Looking for " + attrib.findName + " @ " + connection.rawUrl);
					try {
						Set<ObjectName> instances =
								connection.getServerConnection().queryNames(attrib.findName, null);
						Collectd.logDebug("FastJMX plugin: Found " + instances.size() + " instances of " + attrib.findName + " @ " + connection.rawUrl);
						collectablePermutations.addAll(AttributePermutation.create(instances.toArray(new ObjectName[instances.size()]), connection, attrib, interval, intervalUnit));
					} catch (IOException ioe) {
						Collectd.logError("FastJMX plugin: Failed to find " + attrib.findName + " @ " + connection.rawUrl + " Exception message: " + ioe.getMessage());
					}
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
		synchronized (attributes) {
			for (Attribute attribute : attributes) {
				// If the host is supposed to collect this attribute, and the objectName matches the attribute, add the permutation.
				if (connection.beanAliases.contains(attribute.beanAlias) && attribute.findName.apply(objectName)) {
					collectablePermutations.addAll(AttributePermutation.create(new ObjectName[]{objectName}, connection, attribute, interval, intervalUnit));
				}
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
}