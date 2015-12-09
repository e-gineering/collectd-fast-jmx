package com.e_gineering;

import javax.management.ListenerNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.Notification;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.remote.JMXConnectionNotification;
import javax.management.remote.JMXConnector;
import javax.security.auth.Subject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by bvarner on 11/13/14.
 */
public class SynchronousConnectorAdapter implements JMXConnector {

	Vector<NotificationDef> listeners = new Vector<NotificationDef>();
	AtomicLong notificatioCounter = new AtomicLong();

	private JMXConnector delegate;
	private Connection manager;

	public SynchronousConnectorAdapter(final JMXConnector connector, Connection manager) {
		this.delegate = connector;
		this.manager = manager;
	}

	private void fireNotification(Notification n) {
		synchronized (listeners) {
			NotificationDef nd = null;
			for (int i = listeners.size() - 1; i >= 0; --i) {
				nd = listeners.get(i);
				if (nd.filter == null || nd.filter.isNotificationEnabled(n)) {
					nd.listener.handleNotification(n, nd.handback);
				}
			}
		}
	}

	public void connect() throws IOException {
		this.connect(null);
	}

	public void connect(Map<String, ?> env) throws IOException {
		try {
			delegate.connect(env);
			// Force setting the MBeanServerConnection prior to invoking the callbacks. This avoids a potential race.
			manager.setMBeanServerConnection(delegate.getMBeanServerConnection());
			fireNotification(new JMXConnectionNotification(JMXConnectionNotification.OPENED, this, getConnectionId(), notificatioCounter.getAndIncrement(), "FastJMX SynchronousConnectorAdapter OPENED.", null));
		} catch (IOException ioe) {
			throw ioe;
		} catch (Exception e) { // Hack hack hackity hack hack hack
			// This is a workaround for: https://issues.jboss.org/browse/REMJMX-90
			throw new IOException(e);
		}
	}

	public MBeanServerConnection getMBeanServerConnection() throws IOException {
		return delegate.getMBeanServerConnection();
	}

	public MBeanServerConnection getMBeanServerConnection(Subject delegationSubject) throws IOException {
		return delegate.getMBeanServerConnection(delegationSubject);
	}

	public void close() throws IOException {
		fireNotification(new JMXConnectionNotification(JMXConnectionNotification.CLOSED, this, getConnectionId(), notificatioCounter.getAndIncrement(), "FastJMX SynchronousConnectorAdapter CLOSED.", null));
		delegate.close();
	}

	public void addConnectionNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback) {
		NotificationDef nd = new NotificationDef(listener, filter, handback);
		synchronized(listeners) {
			if (!listeners.contains(nd)) {
				listeners.add(nd);
			}
		}
	}

	public void removeConnectionNotificationListener(NotificationListener listener) throws ListenerNotFoundException {
		List<NotificationDef> toRemove = new ArrayList<NotificationDef>();
		synchronized (listeners) {
			for (NotificationDef def : listeners) {
				if (def.listener == listener) {
					toRemove.add(def);
				}
			}
			listeners.removeAll(toRemove);
		}
	}

	public void removeConnectionNotificationListener(NotificationListener l, NotificationFilter f, Object handback) throws ListenerNotFoundException {
		listeners.remove(new NotificationDef(l, f, handback));
	}

	public String getConnectionId() throws IOException {
		return delegate.getConnectionId();
	}


	private class NotificationDef {
		NotificationListener listener;
		NotificationFilter filter;
		Object handback;

		NotificationDef(NotificationListener listener, NotificationFilter filter, Object handback) {
			this.listener = listener;
			this.filter = filter;
			this.handback = handback;
		}

		public boolean equals(Object obj) {
			if (obj instanceof NotificationDef) {
				NotificationDef other = (NotificationDef)obj;
				return other.listener == this.listener && other.filter == this.filter && other.handback == this.handback;
			}
			return false;
		}
	}
}
