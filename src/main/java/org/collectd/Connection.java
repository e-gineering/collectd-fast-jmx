package org.collectd;

import javax.management.InstanceNotFoundException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerDelegate;
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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Defines permutations for a host connection.
 */
public class Connection implements NotificationListener {

	private static Logger logger = Logger.getLogger(Connection.class.getName());

	private String hostname;
	private String rawUrl;
	private JMXServiceURL serviceURL;
	private String username;
	private String password;
	private String connectionInstancePrefix;
	private List<String> beanAliases;
	private long ttl;

	private NotificationListener notificationListener;
	private JMXConnector serverConnector;
	private MBeanServerConnection serverConnection;

	private Timer connectTimer;

	public Connection(final NotificationListener notificationListener, final String rawUrl, final String hostname, final JMXServiceURL serviceURL, final String username,
	                  final String password, final String connectionInstancePrefix, final List<String> beanAliases, final long ttl) {
		this.notificationListener = notificationListener;
		this.rawUrl = rawUrl;
		this.hostname = hostname;
		this.serviceURL = serviceURL;
		this.username = username;
		this.password = password;
		this.connectionInstancePrefix = connectionInstancePrefix;
		this.beanAliases = beanAliases;
		this.ttl = ttl;

		this.serverConnector = null;
		this.serverConnection = null;
		this.connectTimer = new Timer("Connect-" + rawUrl, true);
	}

	public void connect() {
		if (logger.isLoggable(Level.FINE)) {
			logger.fine("connect() for " + rawUrl);
		}
		ConnectTask task = new ConnectTask(0);
		connectTimer.schedule(task, task.getDelay());
	}

	/**
	 * Removes all NofiticationListeners and closes the connections.
	 */
	public void close() {
		if (logger.isLoggable(Level.FINE)) {
			logger.fine("Closing: " + rawUrl);
		}
		if (serverConnector != null) {
			if (logger.isLoggable(Level.FINE)) {
				logger.fine("Removing connection listeners for " + rawUrl);
			}

			try {
				serverConnector.removeConnectionNotificationListener(notificationListener);
				serverConnector.removeConnectionNotificationListener(this);
			} catch (ListenerNotFoundException lnfe) {
				logger.severe("Failed to unregister connection listeners for " + rawUrl);
			}

			try {
				serverConnector.close();
			} catch (IOException ioe) {
				logger.warning("Exception closing JMXConnection: " + ioe.getMessage());
			}
		}

		serverConnection = null;
		serverConnector = null;
	}


	public MBeanServerConnection getServerConnection() throws IOException {
		if (serverConnector == null && serverConnection == null) {
			throw new IOException("Not Connected to: " + rawUrl);
		} else if (serverConnector != null && serverConnection == null) {
			logger.warning("Returning serverConnector.getMbeanServerConnection(). POSSIBLE RACE.");
			return serverConnector.getMBeanServerConnection();
		}
		return serverConnection;
	}

	/**
	 * Cleans up the serverConnection if we're closed or fail.
	 *
	 * @param notification
	 * @param handback
	 */
	public void handleNotification(final Notification notification, final Object handback) {
		if (notification instanceof JMXConnectionNotification) {
			if (notification.getType().equals(JMXConnectionNotification.CLOSED) ||
					    notification.getType().equals(JMXConnectionNotification.FAILED)) {
				close();
			} else if (notification.getType().equals(JMXConnectionNotification.OPENED)) {
				try {
					serverConnection = serverConnector.getMBeanServerConnection();
					serverConnection.addNotificationListener(MBeanServerDelegate.DELEGATE_NAME, notificationListener, null, this);
					if (ttl > 0) {
						connectTimer.schedule(new ReconnectTask(), TimeUnit.MILLISECONDS.convert(ttl, TimeUnit.SECONDS));
					}
				} catch (IOException ioe) {
					logger.warning("Could not get mbeanServerConnection to: " + rawUrl + " exception message: " + ioe.getMessage());
					close();
					ConnectTask backoffConnect = new ConnectTask(0);
					connectTimer.schedule(backoffConnect, backoffConnect.getDelay());
				} catch (InstanceNotFoundException infe) {
					logger.config("Could not register MBeanServerDelegate. FastJMX will be unable to detect newly deployed or undeployed beans at: " + rawUrl + ".\n" +
							              "You can configure a 'ttl' for this connection to force reconnection and rediscovery of MBeans on a periodic basis.");
				}
			}
		}
	}

	public String getRawUrl() {
		return rawUrl;
	}

	public String getHostname() {
		return hostname;
	}

	public String getConnectionInstancePrefix() {
		return connectionInstancePrefix;
	}

	public List<String> getBeanAliases() {
		return beanAliases;
	}

	@Override
	public int hashCode() {
		return (rawUrl + username + password + hostname + connectionInstancePrefix).hashCode();
	}

	@Override
	public boolean equals(final Object obj) {
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

	void scheduleReconnect() {
		connectTimer.schedule(new ReconnectTask(), 0);
	}

	private class ReconnectTask extends TimerTask {
		public void run() {
			logger.info("Connection TTL expired for " + rawUrl + " forcing reconnect..");
			try {
				serverConnector.close();
			} catch (IOException ioe) {
				logger.severe("Failure to close for TTL reconnect to: " + rawUrl);
			}

		}
	}


	private class ConnectTask extends TimerTask {
		private int connectBackoff = 0;

		private ConnectTask(final int backoffSeconds) {
			this.connectBackoff = backoffSeconds;
		}

		public long getDelay() {
			return TimeUnit.MILLISECONDS.convert(connectBackoff, TimeUnit.SECONDS);
		}

		@Override
		public void run() {
			this.cancel();
			logger.info("Connecting to: " + rawUrl);

			if (connectBackoff == 0) {
				connectBackoff = 5;
			} else {
				connectBackoff *= connectBackoff / (connectBackoff / 2);
			}
			// Clamp the backoff to 5 minutes.
			if (connectBackoff > 300) {
				connectBackoff = 300;
			}

			// If we don't have a serverConnector, try to set one up and subscribe a listener.
			if (serverConnector == null) {
				Map environment = new HashMap();
				if (password != null && username != null) {
					environment.put(JMXConnector.CREDENTIALS, new String[]{username, password});
				}
				environment.put(JMXConnectorFactory.PROTOCOL_PROVIDER_CLASS_LOADER, this.getClass().getClassLoader());

				try {
					serverConnector = JMXConnectorFactory.newJMXConnector(serviceURL, environment);
					serverConnector.addConnectionNotificationListener(Connection.this, null, null);
					serverConnector.addConnectionNotificationListener(notificationListener, null, Connection.this);
					serverConnector.connect();
					if (logger.isLoggable(Level.FINE)) {
						logger.fine("ServerConnector connect() invoked.");
					}
				} catch (IOException ioe) {
					logger.warning("Could not connect to : " + rawUrl + " exception message: " + ioe.getMessage());
					close();
					logger.info("Scheduling reconnect to: " + rawUrl + " in " + connectBackoff + " seconds.");
					ConnectTask backoffConnect = new ConnectTask(connectBackoff);
					connectTimer.schedule(backoffConnect, backoffConnect.getDelay());
				}
			}
		}
	}
}
