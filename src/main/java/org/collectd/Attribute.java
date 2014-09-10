package org.collectd;

import org.collectd.api.DataSet;

import javax.management.ObjectName;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Defines the parameters needed to build a set of AttributePermutations combined with a ConnectionDefinition.
 */
public class Attribute {
	String beanAlias;
	String pluginName;

	ObjectName findName;
	String beanInstancePrefix;
	List<String> beanInstanceFrom = new ArrayList<String>();

	LinkedHashMap<String, List<String>> attributes = new LinkedHashMap<String, List<String>>();
	DataSet dataset;
	String valueInstancePrefix;
	List<String> valueInstanceFrom = new ArrayList<String>();
	boolean composite;

	public Attribute(final List<String> attributes, final String pluginName, final DataSet dataset,
	                 final String valueInstancePrefix, final List<String> valueInstanceFrom, final boolean composite,
	                 final String beanAlias, final ObjectName findName,
	                 final String beanInstancePrefix, final List<String> beanInstanceFrom) {
		this.beanAlias = beanAlias;
		this.pluginName = pluginName;
		this.findName = findName;
		this.beanInstancePrefix = beanInstancePrefix;
		this.beanInstanceFrom = beanInstanceFrom;
		for (String attribute : attributes) {
			this.attributes.put(attribute, Arrays.asList(attribute.split("\\.")));
		}
		this.dataset = dataset;
		this.valueInstancePrefix = valueInstancePrefix;
		this.valueInstanceFrom = valueInstanceFrom;
		this.composite = composite;
	}


}
