import org.collectd.api.Collectd;

import javax.management.InstanceNotFoundException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerDelegate;
import javax.management.MBeanServerNotification;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.remote.JMXConnectionNotification;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Defines permutations for a host connection.
 */
public class Connection implements NotificationListener {
	String hostname;
	String rawUrl;
	JMXServiceURL serviceURL;
	String username;
	String password;
	String connectionInstancePrefix;
	List<String> beanAliases;
	private int connectBackoff;

	private NotificationListener notificationListener;
	private JMXConnector serverConnector;
	private MBeanServerConnection serverConnection;

	public Connection(final NotificationListener notificationListener, final String rawUrl, final String hostname, final JMXServiceURL serviceURL, final String username,
	                  final String password, final String connectionInstancePrefix, final List<String> beanAliases) {
		this.notificationListener = notificationListener;
		this.rawUrl = rawUrl;
		this.hostname = hostname;
		this.serviceURL = serviceURL;
		this.username = username;
		this.password = password;
		this.connectionInstancePrefix = connectionInstancePrefix;
		this.beanAliases = beanAliases;

		this.connectBackoff = 0;
		this.serverConnector = null;
		this.serverConnection = null;
	}

	public boolean isConnected() {
		return serverConnector != null && serverConnection != null;
	}

	public void connect() throws IOException {
		if (!isConnected()) {
			synchronized (this) {
				if (connectBackoff == 0) {
					connectBackoff = 5;
				} else {
					Collectd.logNotice("FastJMX plugin: Attempting reconnect to: " + rawUrl + " in: " + connectBackoff + " seconds.");
					try {
						TimeUnit.SECONDS.sleep(connectBackoff);
						connectBackoff *= connectBackoff / (connectBackoff / 2);
					} catch (InterruptedException ie) {
						Collectd.logNotice("FastJMX plugin: Reconnect to: " + rawUrl + " INTERRUPTED.");
						return;
					}
				}

				Map environment = new HashMap();

				if (password != null && username != null) {
					environment.put(JMXConnector.CREDENTIALS, new String[]{username, password});

				}
				environment.put(JMXConnectorFactory.PROTOCOL_PROVIDER_CLASS_LOADER, this.getClass().getClassLoader());

				// If we don't have a serverConnector, try to set one up and subscribe a listener.
				if (serverConnector == null) {
					try {
						serverConnector = JMXConnectorFactory.newJMXConnector(serviceURL, environment);
						serverConnector.addConnectionNotificationListener(notificationListener, null, this);
						serverConnector.addConnectionNotificationListener(this, null, null);
						serverConnector.connect();
					} catch (IOException ioe) {
						serverConnector = null;
						serverConnection = null;

						Collectd.logWarning("FastJMX plugin: Could not connect to : " + rawUrl + " exception message: " + ioe.getMessage());
						throw ioe;
					}
				}

				if (serverConnection == null && serverConnector != null) {
					try {
						MBeanServerConnection connection = getServerConnection();
						connection.addNotificationListener(MBeanServerDelegate.DELEGATE_NAME, notificationListener, null, this);
					} catch (IOException ioe) {
						Collectd.logWarning("FastJMX plugin: Could not get mbeanServerConnection to: " + rawUrl + " exception message: " + ioe.getMessage());
						close();
						throw ioe;
					} catch (InstanceNotFoundException infe) {
						Collectd.logNotice("FastJMX plugin: Could not register MBeanServerDelegate. I will not be able to detect newly deployed or undeployed beans at: " + rawUrl);
					}
				}
			}
		}
		return;
	}

	/**
	 * Removes all NofiticationListeners and closes the connections.
	 */
	public void close() {
		Collectd.logDebug("FastJMX plugin: Closing: " + rawUrl);
		if (serverConnector != null) {
			Collectd.logDebug("FastJMX plugin: Removing connection listeners for " + rawUrl);
			try {
				serverConnector.removeConnectionNotificationListener(notificationListener);
				serverConnector.removeConnectionNotificationListener(this);
			} catch (ListenerNotFoundException lnfe) {
				Collectd.logDebug("FastJMX plugin: Couldn't unregister ourselves from our JMXConnector.");
			}

			try {
				serverConnector.close();
			} catch (IOException ioe) {
				Collectd.logWarning("FastJMX plugin: Exception closing JMXConnection: " + ioe.getMessage());
			}
		}

		serverConnection = null;
		serverConnector = null;
	}


	public MBeanServerConnection getServerConnection() throws BrokenConnectionException {
		if (serverConnector == null && serverConnection == null) {
			throw new BrokenConnectionException(this, "Broken Connection to: " + rawUrl);
		} else if (serverConnector != null && serverConnection == null) {
			synchronized (this) {
				if (serverConnection == null) { // Another thread may have set this before we got the lock.
					try {
						serverConnection = serverConnector.getMBeanServerConnection();
						connectBackoff = 0;
					} catch (IOException ioe) {
						close();
						throw new BrokenConnectionException(this, "Broken Connection to: " + rawUrl, ioe);
					}
				}
			}
		}
		return serverConnection;
	}

	/**
	 * Cleans up the serverConnection if we're closed or fail.
	 *
	 * @param notification
	 * @param handback
	 */
	public void handleNotification(Notification notification, Object handback) {
		if (notification instanceof JMXConnectionNotification &&
				    (notification.getType().equals(JMXConnectionNotification.CLOSED) ||
						     notification.getType().equals(JMXConnectionNotification.FAILED))) {
			serverConnection = null;
		}
	}


	@Override
	public int hashCode() {
		return (rawUrl + username + password + hostname + connectionInstancePrefix).hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} else if (obj instanceof Connection) {
			Connection that = (Connection) obj;
			return that.rawUrl.equals(this.rawUrl) &&
					       this.username != null ? this.username.equals(that.username) : this.username == that.username &&
							                                                                     this.password != null ? this.password.equals(that.password) : this.password == that.password &&
									                                                                                                                                   this.hostname != null ? this.hostname.equals(that.hostname) : this.hostname == that.hostname &&
											                                                                                                                                                                                                 this.connectionInstancePrefix != null ? this.connectionInstancePrefix.equals(that.connectionInstancePrefix) : this.connectionInstancePrefix == this.connectionInstancePrefix;
		}
		return false;
	}
}
