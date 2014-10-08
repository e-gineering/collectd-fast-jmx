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
import java.util.concurrent.TimeUnit;

/**
 * FastJMX is a Collectd plugin that allows for lower latency read cycles on remote JMX hosts.
 * It does so by maintaining a threadpool which is used to read attribute values from remote hosts during a read
 * cycle. The thread pool uses a histogram of the recent collections to project the most efficient pool size to
 * minimize read latency.
 */
public class FastJMX implements CollectdConfigInterface, CollectdInitInterface, CollectdReadInterface, CollectdShutdownInterface, NotificationListener {

	private long reads;
	private SelfTuningCollectionExecutor executor;

	private static List<Attribute> attributes = new ArrayList<Attribute>();
	private static HashSet<Connection> connections = new HashSet<Connection>();

	private static List<AttributePermutation> collectablePermutations =
			Collections.synchronizedList(new ArrayList<AttributePermutation>(100));

	static {
		System.getProperties().put("sun.rmi.transport.tcp.connectTimeout", TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS));
		System.getProperties().put("sun.rmi.transport.tcp.handshakeTimeout", TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS));
		System.getProperties().put("sun.rmi.transport.tcp.responseTimeout", TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS));
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
	 * <li>MaxThreads: Changes the maximum number of threads to allow. Default is 512.</li>
	 * <li>CollectInternal: Reports internal metrics from FastJMX back to Collectd.</li>
	 * <li>"MBean", "MXBean", and "Bean" are now interchangeable. This plugin also works with MXBeans.</li>
	 * <li>The Hostname is auto-detected from the ServiceURL unless specified as the "string" portion of the org.collectd.Connection
	 * definition. The "host" property is still supported for backwards-compatibility.</li>
	 * <li>The Value block's string value is added to the "org.collectd.Attribute" list. The "org.collectd.Attribute" property is still supported for
	 * backards-compatibility.</li>
	 * <li>"table" and "composite" are aliases for each other.</li>
	 * <li>"user" and "username" are aliases for each other.</li>
	 * <li></li>
	 * </ul>
	 * <p/>
	 * <pre>
	 * {@Code
	 * <Plugin "FastJMX">
	 *   MaxThreads 384
	 *   CollectInternal true
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
		int maxThreads = 512;
		boolean collectInternal = false;

		for (OConfigItem pluginChild : ci.getChildren()) {
			if ("maxthreads".equalsIgnoreCase(pluginChild.getKey())) {
				maxThreads = getConfigInt(pluginChild, maxThreads);
			} else if ("collectinternal".equalsIgnoreCase(pluginChild.getKey())) {
				collectInternal = getConfigBoolean(pluginChild);
			} else if ("mbean".equalsIgnoreCase(pluginChild.getKey()) || "mxbean".equalsIgnoreCase(pluginChild.getKey()) || "bean".equalsIgnoreCase(pluginChild.getKey())) {
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

		this.executor = new SelfTuningCollectionExecutor(maxThreads, collectInternal);

		return 0;
	}


	/**
	 * Attempts to open connections to all configured Connections.
	 */
	public int init() {
		this.reads = 0;

		// Open connections.
		for (Connection connectionEntry : connections) {
			connectionEntry.connect();
		}

		return 0;
	}


	/**
	 * Attempts to read all identified permutations of beans for each connection before the next (interval - 500ms).
	 * Any attributes not read by that time will be cancelled and no metrics will be gathered for those points.
	 *
	 * @return
	 */
	public int read() {
		Collectd.logDebug("FastJMX plugin: read()...");
		try {
			// Rollover in case we're _really_ long running.
			if (reads++ == Long.MAX_VALUE) {
				reads = 1;
			}

			synchronized (collectablePermutations) {
				// Make sure the most latent attributes are the first ones to be collected.
				Collections.sort(collectablePermutations);
				try {
					executor.invokeAll(collectablePermutations);
				} catch (InterruptedException ie) {
					Collectd.logNotice("FastJMX plugin: Interrupted during read() cycle.");
				}
			}
		} catch (Throwable t) {
			Collectd.logError("FastJMX plugin: Unexpected Throwable: " + t);
		}
		return 0;
	}

	// Remove any notification listeners... clean up our stuffs then stop.
	public int shutdown() {
		executor.shutdown();
		return 0;
	}

	/**
	 * Creates AttributePermutations for the given connection, querying the remote MBeanServer for ObjectNames matching
	 * defined collect attributes for the connection.
	 *
	 * @param connection The Connection to create permutations and start collecting.
	 */
	private void createPermutations(final Connection connection) {
		Collectd.logDebug("FastJMX plugin: Creating AttributePermutations for " + connection.getRawUrl());
		// Create the org.collectd.AttributePermutation objects appropriate for this org.collectd.Connection.
		for (Attribute attrib : attributes) {
			// If the host is supposed to collect this attribute, look for matching objectNames on the host.
			if (connection.getBeanAliases().contains(attrib.getBeanAlias())) {
				Collectd.logDebug("FastJMX plugin: Looking for " + attrib.getObjectName() + " @ " + connection.getRawUrl());
				try {
					Set<ObjectName> instances =
							connection.getServerConnection().queryNames(attrib.getObjectName(), null);
					Collectd.logDebug("FastJMX plugin: Found " + instances.size() + " instances of " + attrib.getObjectName() + " @ " + connection.getRawUrl());
					collectablePermutations.addAll(AttributePermutation.create(instances.toArray(new ObjectName[instances.size()]), connection, attrib));
				} catch (IOException ioe) {
					Collectd.logError("FastJMX plugin: Failed to find " + attrib.getObjectName() + " @ " + connection.getRawUrl() + " Exception message: " + ioe.getMessage());
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
		Collectd.logDebug("FastJMX plugin: Removing AttributePermutations for " + connection.getRawUrl());
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
		for (Attribute attribute : attributes) {
			// If the host is supposed to collect this attribute, and the objectName matches the attribute, add the permutation.
			if (connection.getBeanAliases().contains(attribute.getBeanAlias()) && attribute.getObjectName().apply(objectName)) {
				collectablePermutations.addAll(AttributePermutation.create(new ObjectName[]{objectName}, connection, attribute));
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
	 * Gets the first value (if it exists) from the OConfigItem as an int.
	 *
	 * @param ci
	 * @param def A default value to return if no Number is found.
	 * @return The int, or the value of <code>def</code> if no int is found.
	 */
	private static int getConfigInt(final OConfigItem ci, final int def) {
		List<OConfigValue> values;
		OConfigValue v;

		values = ci.getValues();
		if (values.size() != 1) {
			return def;
		}

		v = values.get(0);
		if (v.getType() != OConfigValue.OCONFIG_TYPE_NUMBER) {
			return def;
		}
		return v.getNumber().intValue();
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