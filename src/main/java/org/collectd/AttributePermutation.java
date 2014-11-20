package org.collectd;

import org.collectd.api.DataSource;
import org.collectd.api.PluginData;
import org.collectd.api.ValueList;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenType;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Defines an actual permutation of an org.collectd.Attribute to be read from a org.collectd.Connection.
 */
public class AttributePermutation implements Callable<AttributePermutation>, Comparable<AttributePermutation> {
	private static Logger logger = Logger.getLogger(AttributePermutation.class.getName());

	private ObjectName objectName;
	private Connection connection;
	private Attribute attribute;
	private PluginData pluginData;
	private ValueList valueList;

	private long lastRunDuration = 0l;
	private boolean interruptedOrFailed = false;
	private int consecutiveNotFounds = 0;
	private List<ValueList> dispatch = new ArrayList<ValueList>(1);

	private AttributePermutation(final ObjectName objectName, final Connection connection, final Attribute attribute, final PluginData pd, final ValueList vl) {
		this.objectName = objectName;
		this.connection = connection;
		this.attribute = attribute;
		this.pluginData = pd;
		this.valueList = vl;
	}

	public static List<AttributePermutation> create(final ObjectName[] objectNames, final Connection connection, final Attribute context) {
		// This method takes into account the beanInstanceFrom and valueInstanceFrom properties to create many AttributePermutations.
		if (objectNames.length == 0) {
			logger.warning("No MBeans matched " + context.getObjectName() + " @ " + connection.getRawUrl());
			return new ArrayList<AttributePermutation>(0);
		}

		List<AttributePermutation> permutations = new ArrayList<AttributePermutation>();

		PluginData pd = new PluginData();
		pd.setHost(connection.getHostname());
		if (context.getPluginName() != null) {
			pd.setPlugin(context.getPluginName());
		} else {
			pd.setPlugin("FastJMX");
		}

		for (ObjectName objName : objectNames) {
			PluginData permutationPD = new PluginData(pd);
			List<String> beanInstanceList = new ArrayList<String>();
			StringBuilder beanInstance = new StringBuilder();

			for (String propertyName : context.getBeanInstanceFrom()) {
				String propertyValue = objName.getKeyProperty(propertyName);

				if (propertyValue == null) {
					logger.severe("No such property [" + propertyName + "] in ObjectName [" + objName + "] for bean instance creation.");
				} else {
					beanInstanceList.add(propertyValue);
				}
			}

			if (connection.getConnectionInstancePrefix() != null) {
				beanInstance.append(connection.getConnectionInstancePrefix());
			}

			if (context.getBeanInstancePrefix() != null) {
				if (beanInstance.length() > 0) {
					beanInstance.append("-");
				}
				beanInstance.append(context.getBeanInstancePrefix());
			}

			for (int i = 0; i < beanInstanceList.size(); i++) {
				if (i > 0) {
					beanInstance.append("-");
				}
				beanInstance.append(beanInstanceList.get(i));
			}
			permutationPD.setPluginInstance(beanInstance.toString());

			ValueList vl = new ValueList(permutationPD);
			vl.setType(context.getDataSet().getType());

			List<String> attributeInstanceList = new ArrayList<String>();
			for (String propertyName : context.getValueInstanceFrom()) {
				String propertyValue = objName.getKeyProperty(propertyName);
				if (propertyValue == null) {
					logger.severe("no such property [" + propertyName + "] in ObjectName [" + objName + "] for attribute instance creation.");
				} else {
					attributeInstanceList.add(propertyValue);
				}
			}

			StringBuilder attributeInstance = new StringBuilder();
			if (context.getValueInstancePrefix() != null) {
				attributeInstance.append(context.getValueInstancePrefix());
			}

			for (int i = 0; i < attributeInstanceList.size(); i++) {
				if (i > 0) {
					attributeInstance.append("-");
				}
				attributeInstance.append(attributeInstanceList.get(i));
			}
			vl.setTypeInstance(attributeInstance.toString());

			permutations.add(new AttributePermutation(objName, connection, context, permutationPD, vl));
		}

		return permutations;
	}

	public Connection getConnection() {
		return connection;
	}

	public ObjectName getObjectName() {
		return objectName;
	}

	public List<ValueList> getValues() {
		return dispatch;
	}

	/**
	 * Implements Comparable, allowing for a natural sort ordering of previous <em>successful</em> execution duration.
	 * <p/>
	 * Executions previously cancelled or failed will be treated as 'not run', and have a duration of '0', making them
	 * 'less than' by comparison. If both objects being compared have a run duration of 0, they are sorted according to
	 * the computed hashCode() values.
	 *
	 * @param o
	 * @return
	 */
	public int compareTo(final AttributePermutation o) {

		int i = -1 * Long.valueOf(getLastRunDuration()).compareTo(Long.valueOf(o.getLastRunDuration())); 				
		if (i != 0) {
			return i;
		}

		return Integer.valueOf(hashCode()).compareTo(Integer.valueOf(o.hashCode()));
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} else if (obj instanceof AttributePermutation) {
			return hashCode() == obj.hashCode();
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return (connection.getHostname() + connection.getRawUrl() + objectName.toString() + pluginData.getSource() + valueList.getType()).hashCode();
	}

	/**
	 * Reads the attribute from the JMX org.collectd.Connection and submits it back to Collectd.
	 *
	 * @return
	 * @throws Exception
	 */
	public AttributePermutation call() throws Exception {
		long start = System.nanoTime();
		dispatch.clear();
		// Snapshot the value list for this 'call', Value lists are built at the end of the call(),
		// and if another thread call()s while this one is still running, it could trounce the interval
		// and report back duplicates to collectd.
		ValueList callVal = new ValueList(this.valueList);
		interruptedOrFailed = true;
		try {
			MBeanServerConnection mbs = connection.getServerConnection();

			List<Object> values = new ArrayList<Object>(8);
			for (Map.Entry<String, List<String>> attributePath : attribute.getAttributes().entrySet()) {
				Object value = null;
				StringBuilder path = new StringBuilder();

				for (int i = 0; i < attributePath.getValue().size(); i++) {
					String node = attributePath.getValue().get(i);
					// If this is our first loop over the path, just get the attribute into the value object.
					if (i == 0) {
						path.append(node);

						try {
							value = mbs.getAttribute(objectName, node);
						} catch (AttributeNotFoundException anfe) {
							value = mbs.invoke(objectName, node, null, null);
						}
						consecutiveNotFounds = 0;
						if (Thread.currentThread().isInterrupted()) {
							return this;
						}
					} else {
						path.append(".").append(node);

						// Subsequent path traversals mean we need to inspect the value object and take appropriate action.
						if (value instanceof CompositeData) {
							CompositeData compositeValue = (CompositeData) value;
							value = compositeValue.get(node);
						} else if (value instanceof OpenType) {
							throw new UnsupportedOperationException("Handling of OpenType " + ((OpenType) value).getTypeName() + " is not yet implemented.");
						} else if (value != null) {
							// Try to traverse via Reflection.
							value = value.getClass().getDeclaredField(node).get(value);
						} else if (i + 1 == attributePath.getValue().size()) {
							// TODO: Configure this so users can try to track down what isn't working.
							// It's really annoying though, for things like LastGcInfo.duration, which are transient for things like CMS collectors.
							if (logger.isLoggable(Level.FINE)) {
								logger.fine("NULL read from " + path + " in " + objectName + " @ " + connection.getRawUrl());
							}
						}
					}
				}
				values.add(value);
			}

			if (Thread.currentThread().isInterrupted()) {
				return this;
			}

			// If we're expecting CompositeData objects to be brokenConnection up like a table, handle it.
			if (attribute.isComposite()) {
				List<CompositeData> cdList = new ArrayList<CompositeData>();
				Set<String> keys = null;

				for (Object obj : values) {
					if (obj instanceof CompositeData) {
						if (keys == null) {
							keys = ((CompositeData) obj).getCompositeType().keySet();
						}
						cdList.add((CompositeData) obj);
					} else {
						throw new IllegalArgumentException("At least one of the attributes from " + objectName + " @ " + connection.getRawUrl() + " was not a 'CompositeData' as requried when table|composite = 'true'");
					}
				}

				for (String key : keys) {
					ValueList vl = new ValueList(callVal);
					vl.setTypeInstance(vl.getTypeInstance() + key);
					vl.setValues(genericCompositeToNumber(cdList, key));
					if (logger.isLoggable(Level.FINEST)) {
						logger.finest("dispatch " + vl);
					}
					dispatch.add(vl);
				}
			} else if (!values.contains(null)) {
				ValueList vl = new ValueList(callVal);
				vl.setValues(genericListToNumber(values));
				if (logger.isLoggable(Level.FINEST)) {
					logger.finest("dispatch " + vl);
				}
				dispatch.add(vl);
			}
			interruptedOrFailed = false;
		} catch (IOException ioe) {
			// This normally comes about as an issue with the underlying connection. Specifically in debugging JBoss
			// remoting connections, we may end up getting subclasses of IOException when the remoting connection has
			// failed. I believe the best way to handle this is with the existing 'not found' reconnect behavior.
			consecutiveNotFounds++;
		} catch (InstanceNotFoundException infe) {
			// The FastJMX plugin wasn't notified of the mbean unregister prior to the collect cycle.
			// This can be a valid case, if the server unregistered the bean during a read cycle (when the collection
			// of AttributePermutations is locked) -- and should result in the AttributePermutation being removed before
			// the next collect cycle. (proper behavior).
			//
			// If for some reason this exception occurs in consecutive read cycles, consider it a suspect server
			// implementation and schedule a reconnect for the connection, so we re-discover the MBeans on the server.
			// Turns out that this is incredibly common with JBoss AS >= EAP 6.x (7.x community and WildFly) due to the
			// transition to their remoting-jmx provider, which does not support MBeanDelegate listeners (at this time)
			//
			consecutiveNotFounds++;
		} catch (Exception ex) {
			throw ex;
		} finally {
			lastRunDuration = System.nanoTime() - start;
		}

		return this;
	}

	int getConsecutiveNotFounds() {
		return consecutiveNotFounds;
	}

	public long getLastRunDuration() {
		if (!interruptedOrFailed) {
			return lastRunDuration;
		}
		return 0l;
	}

	private List<Number> genericCompositeToNumber(final List<CompositeData> cdlist, final String key) {
		List<Object> objects = new ArrayList<Object>();

		for (int i = 0; i < cdlist.size(); i++) {
			CompositeData cd;
			Object value;

			cd = cdlist.get(i);
			value = cd.get(key);
			objects.add(value);
		}

		return genericListToNumber(objects);
	}


	private List<Number> genericListToNumber(final List<Object> objects) throws IllegalArgumentException {
		List<Number> ret = new ArrayList<Number>();
		List<DataSource> dsrc = this.attribute.getDataSet().getDataSources();

		for (int i = 0; i < objects.size(); i++) {
			ret.add(genericObjectToNumber(objects.get(i), dsrc.get(i).getType()));
		}

		return ret;
	}

	/**
	 * Converts a generic (OpenType) object to a number.
	 * <p/>
	 * Returns null if a conversion is not possible or not implemented.
	 */
	private Number genericObjectToNumber(final Object obj, final int ds_type) throws IllegalArgumentException {
		if (obj instanceof String) {
			String str = (String) obj;

			try {
				if (ds_type == DataSource.TYPE_GAUGE) {
					return (new Double(str));
				} else {
					return (new Long(str));
				}
			} catch (NumberFormatException e) {
				return (null);
			}
		} else if (obj instanceof Byte) {
			return (new Byte((Byte) obj));
		} else if (obj instanceof Short) {
			return (new Short((Short) obj));
		} else if (obj instanceof Integer) {
			return (new Integer((Integer) obj));
		} else if (obj instanceof Long) {
			return (new Long((Long) obj));
		} else if (obj instanceof Float) {
			return (new Float((Float) obj));
		} else if (obj instanceof Double) {
			return (new Double((Double) obj));
		} else if (obj instanceof BigDecimal) {
			return (BigDecimal.ZERO.add((BigDecimal) obj));
		} else if (obj instanceof BigInteger) {
			return (BigInteger.ZERO.add((BigInteger) obj));
		}

		throw new IllegalArgumentException("Cannot convert type: " + obj.getClass().getSimpleName() + " to Number.");
	}
}
