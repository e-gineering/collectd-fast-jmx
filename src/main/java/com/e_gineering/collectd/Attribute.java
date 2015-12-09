package com.e_gineering.collectd;

import org.collectd.api.DataSet;

import javax.management.ObjectName;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Defines the parameters needed to build a set of AttributePermutations combined with a ConnectionDefinition.
 */
public class Attribute {
	private String beanAlias;
	private String pluginName;

	private ObjectName findName;
	private String beanInstancePrefix;
	private List<String> beanInstanceFrom = new ArrayList<String>();

	private LinkedHashMap<String, List<String>> attributes = new LinkedHashMap<String, List<String>>();
	private DataSet dataset;
	private String valueInstancePrefix;
	private List<String> valueInstanceFrom = new ArrayList<String>();
	private boolean composite;

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

	public String getBeanAlias() {
		return beanAlias;
	}

	public ObjectName getObjectName() {
		return findName;
	}

	public String getPluginName() {
		return pluginName;
	}

	public List<String> getBeanInstanceFrom() {
		return beanInstanceFrom;
	}

	public DataSet getDataSet() {
		return dataset;
	}

	public String getBeanInstancePrefix() {
		return beanInstancePrefix;
	}

	public List<String> getValueInstanceFrom() {
		return valueInstanceFrom;
	}

	public String getValueInstancePrefix() {
		return valueInstancePrefix;
	}

	public HashMap<String, List<String>> getAttributes() {
		return attributes;
	}

	public boolean isComposite() {
		return composite;
	}

}
