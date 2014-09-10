package org.collectd;

import org.collectd.api.Collectd;
import org.collectd.api.DataSource;
import org.collectd.api.PluginData;
import org.collectd.api.ValueList;

import javax.management.AttributeNotFoundException;
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

/**
 * Defines an actual permutation of an org.collectd.Attribute to be read from a org.collectd.Connection.
 */
public class AttributePermutation implements Callable<AttributePermutation> {
	private ObjectName objectName;
	private Connection connection;
	private Attribute attribute;
	private PluginData pluginData;
	private ValueList valueList;

	private long lastRunDuration = 0l;

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
			Collectd.logWarning("FastJMX plugin:  No MBeans matched " + context.findName + " @ " + connection.rawUrl);
			return new ArrayList<AttributePermutation>(0);
		}

		List<AttributePermutation> permutations = new ArrayList<AttributePermutation>();

		PluginData pd = new PluginData();
		pd.setHost(connection.hostname);
		if (context.pluginName != null) {
			pd.setPlugin(context.pluginName);
		} else {
			pd.setPlugin("FastJMX");
		}

		for (ObjectName objName : objectNames) {
			PluginData permutationPD = new PluginData(pd);
			List<String> beanInstanceList = new ArrayList<String>();
			StringBuilder beanInstance = new StringBuilder();

			for (String propertyName : context.beanInstanceFrom) {
				String propertyValue = objName.getKeyProperty(propertyName);

				if (propertyValue == null) {
					Collectd.logError("FastJMX plugin:  No such property [" + propertyName + "] in ObjectName [" + objName + "] for bean instance creation.");
				} else {
					beanInstanceList.add(propertyValue);
				}
			}

			if (connection.connectionInstancePrefix != null) {
				beanInstance.append(connection.connectionInstancePrefix);
			}

			if (context.beanInstancePrefix != null) {
				if (beanInstance.length() > 0) {
					beanInstance.append("-");
				}
				beanInstance.append(context.beanInstancePrefix);
			}

			for (int i = 0; i < beanInstanceList.size(); i++) {
				if (i > 0) {
					beanInstance.append("-");
				}
				beanInstance.append(beanInstanceList.get(i));
			}
			permutationPD.setPluginInstance(beanInstance.toString());

			ValueList vl = new ValueList(permutationPD);
			vl.setType(context.dataset.getType());

			List<String> attributeInstanceList = new ArrayList<String>();
			for (String propertyName : context.valueInstanceFrom) {
				String propertyValue = objName.getKeyProperty(propertyName);
				if (propertyValue == null) {
					Collectd.logError("FastJMX plugin:  no such property [" + propertyName + "] in ObjectName [" + objName + "] for attribute instance creation.");
				} else {
					attributeInstanceList.add(propertyValue);
				}
			}

			StringBuilder attributeInstance = new StringBuilder();
			if (context.valueInstancePrefix != null) {
				attributeInstance.append(context.valueInstancePrefix);
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

	public Attribute getAttribute() {
		return attribute;
	}

	@Override
	public int hashCode() {
		return (connection.hostname + connection.rawUrl + objectName.toString() + pluginData.getSource() + valueList.getType()).hashCode();
	}

	/**
	 * Reads the attribute from the JMX org.collectd.Connection and submits it back to Collectd.
	 *
	 * @return
	 * @throws Exception
	 */
	public synchronized AttributePermutation call() throws Exception {
		long start = System.nanoTime();
		try {
			MBeanServerConnection mbs = connection.getServerConnection();

			List<Object> values = new ArrayList<Object>(8);
			for (Map.Entry<String, List<String>> attributePath : attribute.attributes.entrySet()) {
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
					} else {
						path.append(".").append(node);

						// Subsequent path traversals mean we need to inspect the value object and take appropriate action.
						if (value instanceof CompositeData) {
							CompositeData compositeValue = (CompositeData) value;
							value = compositeValue.get(node);
						} else if (value instanceof OpenType) {
							throw new UnsupportedOperationException("Handling of OpenType " + ((OpenType) value).getTypeName() + " is not yet implemented.");
						} else {
							// Try to traverse via Reflection.
							value = value.getClass().getDeclaredField(node).get(value);
						}
					}

					if (value == null) {
						throw new IllegalStateException("Could not read " + path + " from " + objectName + " @ " + connection.rawUrl);
					}
				}

				values.add(value);
			}

			// If we're expecting CompositeData objects to be brokenConnection up like a table, handle it.
			if (attribute.composite) {
				List<CompositeData> cdList = new ArrayList<CompositeData>();
				Set<String> keys = null;

				for (Object obj : values) {
					if (obj instanceof CompositeData) {
						if (keys == null) {
							keys = ((CompositeData) obj).getCompositeType().keySet();
						}
						cdList.add((CompositeData) obj);
					} else {
						throw new IllegalArgumentException("At least one of the attributes from " + objectName + " @ " + connection.rawUrl + " was not a 'CompositeData' as requried when table|composite = 'true'");
					}
				}

				for (String key : keys) {
					ValueList vl = new ValueList(this.valueList);
					vl.setTypeInstance(vl.getTypeInstance() + key);
					vl.setValues(genericCompositeToNumber(cdList, key));
					Collectd.logDebug("FastJMX plugin:  dispatch " + vl);
					Collectd.dispatchValues(vl);
				}
			} else {
				ValueList vl = new ValueList(this.valueList);
				vl.setValues(genericListToNumber(values));
				Collectd.logDebug("FastJMX plugin:  dispatch " + vl);
				Collectd.dispatchValues(vl);
			}
		} catch (IOException ioe) {
			throw ioe;
		} catch (Exception ex) {
			throw ex;
		} finally {
			lastRunDuration = System.nanoTime() - start;
		}

		return this;
	}

	public long getLastRunDuration() {
		return lastRunDuration;
	}

	private List<Number> genericCompositeToNumber(List<CompositeData> cdlist, String key) {
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


	private List<Number> genericListToNumber(List<Object> objects) throws IllegalArgumentException {
		List<Number> ret = new ArrayList<Number>();
		List<DataSource> dsrc = this.attribute.dataset.getDataSources();

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
	private Number genericObjectToNumber(Object obj, int ds_type) throws IllegalArgumentException {
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
