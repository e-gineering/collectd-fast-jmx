## FastJMX - Low-latency JMX collectd plugin

The default GenericJMX plugin from collectd is great for basic collection of small numbers of metrics, but if you need to collect many metrics from one 
or more hosts, the latency to read the metrics can quickly exceed your interval time, and that's no fun. If you want to remotely collect metrics from
multiple hosts you can forget about having short intervals, and some of the configuration settings aren't exactly obvious. 
Example: What do you mean I have to include the hostname? I gave you the serviceUrl!

### Introducing FastJMX!

FastJMX does things differently than the GenericJMX plugin, but it does it in a manner that's configuration-compatible with the original plugin.
(You read that right. There's just a few small tweaks to an existing configuration and FastJMX will take over)

* FastJMX discovers all the matching beans when it first connects, then sets up listeners to the remote server so we get callbacks when any beans are added or removed from the server. This lets us identify all the permutations of the beans we need to read outside of the `read()` loop, which reduces `read()` latency, as well as internal GC stress and memory pressure.
* Reconnections are attempted with increasing backoff sleep durations. Again, outside of the read loop, so that collecting metrics from connections which aren't failed continues to work.
* Each attribute read from an mbean is it's own potential thread. The JDK 1.5 Concurrent packages are used to pool threads, inflict interval timeouts on the read cycle, and to make sure the queue is clear at the end of each `read()` invocation eliminating backlogged (lagged) metric reporting. If there isn't a metric polled in a timely manner, it's a dropped read.
* Each `read()` cycle is timeslot protected (synchronized to the interval configured in collectd) so that old values and current values are *never* intermixed.
* Each `<Value>` can define a custom `PluginName`, allowing segementation of reported metrics into different plugin buckets rather than everything being reported as "GenericJMX" or "FastJMX".
* The port can be appended to the hostname using `IncludePortInHostname`. This is very helpful in separating data from multiple JVM instances on the same host without needing to specify an `InstancePrefix` on the `<Connection>`.
* Hostnames are automatically detected from the serviceURL. If the serviceURL is a complex type, like `service:jmx:rmi:///jndi/rmi://hostname:port/jmxrmi`, FastJMX will still properly parse the hostname and port. The `Hostname` property (part of the standard GenericJMX configuration) value is still respected if present.
* FastJMX doesn't require connections be defined after the beans. `<MBean>` (or `<MXBean>`, or just `<Bean>`) and `<Connection>` blocks can come in _any_ order.

### So how much faster is it?

In real-world collection scenarios, large volume remote collections from multiple hosts over a VPN improved from ~2500ms to collect (with GenericJMX) to ~120ms.

If you really want to know what FastJMX is doing, add `CollectInternal true` to the plugin configuration. This tells FastJMX to dispatch internal metrics (success, failure, error, latency, thread pool size) to collectd.

## Configuration
### Migrate from GenericJMX by...

* Add the path to the fast-jmx jar in JVMARG
* Include `LoadPlugin "org.collectd.FastJMX` in the `<Plugin "java">` block.

### Additional FastJMX Options:

* Remove the `hostname` from the `<Connection>` blocks. FastJMX will do it's best to detect it from the jmx URI if you don't include it. If parsing has an issue, you'll see a message in the log.
* Asynch connection handling by default, but you can force synch by adding `Synchronous true` to a `<Connection>` block. If the url contains `remoting-jmx` which is interpreted as [JBoss Remoting](https://github.com/jbossas/remoting-jmx) then the synchronous wrapper is auto-magic-ally enabled.
* Single-attribute `<Value>` blocks can use the syntax `<Value "attributeName">`. See the `<MBean "classes">` example below.
* Include `PluginName` declarations in a `<Value>` block to change the plugin name it's reported as. Useful for grouping different MBeans as if they came from different applications, or subsystems.
* Use `<MBean>` or `<MXBean>` or `<Bean>`.
* `Composite` and `Table` can be used interchangeably within a `<Value>` block, and can be omitted (defaults to `false`).
* `MaxThreads` can change the default maximum number of threads (512) to allow.
* `CollectInternal` enables internal metrics FastJMX uses to be reported back to Collectd.
* `TTL` can be used on a Connection to force a reconnect after `<value>` many seconds have elapsed. This can be handy if your server isn't correctly maintining mbeans after redployments. Keep in mind this is seconds, so '43200' = 12 hours.

```
LoadPlugin java
<Plugin "java">
  JVMARG "-Djava.class.path=/usr/share/collectd/java/collectd-api.jar:/path/to/fast-jmx-1.0-SNAPSHOT.jar"
  
  LoadPlugin "org.collectd.FastJMX"

  <Plugin "FastJMX">

    MaxThreads 256
    CollectInternal true
  
    <MBean "classes">
      ObjectName "java.lang:type=ClassLoading"

      <Value "LoadedClassCount">
        Type "gauge"
        InstancePrefix "loaded_classes"
        PluginName "JVM"
      </Value>
    </MBean>

    # Time spent by the JVM compiling or optimizing.
    <MBean "compilation">
      ObjectName "java.lang:type=Compilation"

      <Value "TotalCompilationTime">
        Type "total_time_in_ms"
        InstancePrefix "compilation_time"
        PluginName "JVM"
      </Value>
    </MBean>

    # Garbage collector information
    <MBean "garbage_collector">
      ObjectName "java.lang:type=GarbageCollector,*"
      InstancePrefix "gc-"
      InstanceFrom "name"

      <Value "CollectionTime">
        Type "total_time_in_ms"
        InstancePrefix "collection_time"
      	PluginName "JVM"
      </Value>
    </MBean>

    # Memory usage by memory pool.
    <MBean "memory_pool">
      ObjectName "java.lang:type=MemoryPool,*"
      InstancePrefix "memory_pool-"
      InstanceFrom "name"

      <Value "Usage">
        Type "java_memory"
        Composite true
        PluginName "JVM"
      </Value>
    </MBean>


    <Connection>
      ServiceURL "service:jmx:rmi:///jndi/rmi://host1:8098/jmxrmi"
      IncludePortInHostname true
      Collect "classes"
      Collect "compilation"
      Collect "garbage_collector"
      Collect "memory_pool"
    </Connection>
    <Connection>
      ServiceURL "service:jmx:rmi:///jndi/rmi://host1:8198/jmxrmi"
      IncludePortInHostname true
      Collect "classes"
      Collect "compilation"
      Collect "garbage_collector"
      Collect "memory_pool"
    </Connection>
    <Connection>
      ServiceURL "service:jmx:rmi:///jndi/rmi://host2:8398/jmxrmi"
      IncludePortInHostname true
      Collect "classes"
      Collect "compilation"
      Collect "garbage_collector"
      Collect "memory_pool"
      # Force the connection to reset every 4 hours.
      TTL 14400
    </Connection>

  </Plugin>
  ```

## Internal Metrics
FastJMX collects some internal metrics that it uses to estimate an efficient pool size.
If you enable internal metric collection (see above configuration options) and have the following types defined in types.db, the data will be submitted to collectd.

```
fastjmx_cycle      value:GAUGE:0:U
fastjmx_latency    value:GAUGE:0:U
```

Once you've got collectd keeping your data, you may find these Collection3 graph configurations useful...
```
<Type fastjmx_cycle>
  Module GenericStacked
  DataSources value
  RRDTitle "FastJMX Reads ({plugin_instance})"
  RRDFormat "%6.1lf"
  DSName "cancelled Incomplete "
  DSName "  success Success    "
  DSName "   failed Failed     "
  DSName "   weight Weight     "
  Order success cancelled failed weight
  Color failed ff0000
  Color cancelled ffb000
  Color success 00e000
  Color weight 0000ff
  Stacking on
</Type>
<Type fastjmx_latency>
  Module GenericStacked
  DataSources value
  RRDTitle "FastJMX Latency ({plugin_instance})"
  RRDFormat "%6.1lf"
  DSName "interval Interval" 
  DSName "duration Latency "
  Order interval duration 
  Color duration ffb000
  Color interval 00e000
  Stacking off
</Type>
```

## JBoss EAP 6.x, AS 7.x
The JBoss remoting JMX provider has been tested with EAP 6.x, and should work properly with AS 7.x as well.
As part of getting this to work, some 'workarounds' are included in the FastJMX code-base, which may also apply to other JMX protocol providers. In the case of the JBoss jmx remoting, appropriate bugs and feature requests have been filed.

### JBoss EAP 6 Classpath
Here's an example JVMArg that works with jboss-eap-6.1
```
<Plugin java>
	JVMArg "-Djava.class.path=/usr/share/collectd/java/collectd-api.jar:/usr/lib/jvm/java-7-oracle/lib/jconsole.jar:/usr/lib/jvm/java-7-oracle/lib/tools.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/remoting-jmx/main/remoting-jmx-1.1.0.Final-redhat-1.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/remoting3/main/jboss-remoting-3.2.16.GA-redhat-1.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/logging/main/jboss-logging-3.1.2.GA-redhat-1.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/xnio/main/xnio-api-3.0.7.GA-redhat-1.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/xnio/nio/main/xnio-nio-3.0.7.GA-redhat-1.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/sasl/main/jboss-sasl-1.0.3.Final-redhat-1.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/marshalling/main/jboss-marshalling-1.3.18.GA-redhat-1.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/marshalling/river/main/jboss-marshalling-river-1.3.18.GA-redhat-1.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/as/cli/main/jboss-as-cli-7.2.1.Final-redhat-10.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/staxmapper/main/staxmapper-1.1.0.Final-redhat-2.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/as/protocol/main/jboss-as-protocol-7.2.1.Final-redhat-10.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/dmr/main/jboss-dmr-1.1.6.Final-redhat-1.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/as/controller-client/main/jboss-as-controller-client-7.2.1.Final-redhat-10.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/threads/main/jboss-threads-2.1.0.Final-redhat-1.jar:/usr/share/collectd/java/fast-jmx-1.1-SNAPSHOT.jar"
	LoadPlugin "org.collectd.FastJMX"
	
	...
	
</Plugin>
```

To connect as an administrator you shoudln't need to change anything in the jboss configuration.
The following Connection block should work in this scenario.
```
<Connection>
	ServiceURL "service:jmx:remoting-jmx://yourhostname:9999"
	Username "admin"
	Password "aR3allyStrongP@sswordThatOthersCanSee"
	ttl 300
	IncludePortInHostname false
	Collect "classes"
	...

</Connection>
```

To connect as a normal application user, and expose JMX over the 'remoting' port in EAP 6.1, your domain (or standalone) configuration should include `use-management-endpoint="false"`, like so:
```
<subsystem xmlns="urn:jboss:domain:jmx:1.2">
  <expose-resolved-model/>
  <expose-expression-model/>
  <remoting-connector use-management-endpoint="false"/>
</subsystem>
```

This changes the port from 9999 (the default management port) to 4447 (the remoting port) and requires an *application* user rather than an *administration* user.

You can add the application user using the 'add-user' script from the JBoss Bin dir:
```
$JBOSS_HOME/bin/add-user.sh --silent -a --user jmx --password <yourpasshere>
```

Then in your Connection block, you can use:
```
<Connection>
	ServiceURL "service:jmx:remoting-jmx://yourhostname:4447"
	Username "jmx"
	Password "!amUnprivi1eged"
	ttl 300
	IncludePortInHostname false
	Collect "classes"
	...
</Connection>
```

Which exposes a non-privileged username / password.

*WARNING WARNING WARNING* This Unprivileged user will be able to invoke MBeans via JMX. *WARNING WARNING WARNING*\

## Debugging & Troubleshooting

There are a couple additional configuration options worth nothing, which are helpful if you're troubleshooting an issue.

* `LogLevel` sets the Plugins internal Java log level. By default this is 'INFO'. Meaning any log message generated internall that's INFO or greater will be logged to Collectd at the approprate (corresponding) Collectd Log Level...
* `ForceLoggingTo` Lets you override the normal behavior of mapping Java log levels to collectd log levels, and forces all java log output to be logged at this collectd level.

So under normal operation, things logged in java as SEVERE are logged at ERROR in Collectd, etc.

Setting `ForceLoggingTo "INFO"` will make all Java logging output log in Collectd at INFO.

If your normal Collectd configuration sets the collectd log level to WARNING, but you want to get 'INFO' from the FastJMX plugin, you can do this:

```
<Plugin "FastJMX">
   LogLevel "INFO"
   ForceLoggingTo "WARNING"

   ...
</Plugin>
```

If you'd like to see FINE logging from FastJMX use:

```
<Plugin "FastJMX">
   LogLevel "FINE"
   ForceLoggingTo "WARNING"
</Plugin>
```

Basically, you're setting the java logger write any messages >= `FINE`, and to write those messages as Collectd `WARNING` messages.
It gives a little more control over the verbosity of this single plugin.

