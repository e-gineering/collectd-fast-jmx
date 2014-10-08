## FastJMX - Low-latency JMX collectd plugin

The default GenericJMX plugin from collectd is great for basic collection of small numbers of metrics, but if you need to collect many metrics from one 
or more hosts, the latency to read the metrics can quickly exceed your interval time, and that's no fun. If you want to remotely collect metrics from
multiple hosts you can forget about having short intervals, and some of the configuration settings aren't exactly obvious. 
Example: What do you mean I have to include the hostname? I gave you the serviceUrl!

### Introducing FastJMX - Async ~~all~~ many of the things!

FastJMX does things differently than the GenericJMX plugin, but it does it in a manner that's configuration-compatible with the original plugin.
(You read that right. There's just a few small tweaks to an existing configuration and FastJMX will take over)

* FastJMX discovers all the matching beans when it first connects, then sets up listeners to the remote server so we get callbacks when any beans are added or removed from the server. This lets us identify all the permutations of the beans we need to read outside of the `read()` loop.
* Reconnections are attempted with increasing backoff sleep durations. Again, outside of the read loop, so no other servers are blocked.
* Each attribute read from an mbean is it's own potential thread. The JDK 1.5 Concurrent packages are used to pool threads, inflict interval timeouts on the read cycle, and to make sure the queue is clear at the end of each `read()` invocation eliminating backlogged metric reporting.
* Each `read()` cycle is timeslot protected (synched to the interval configured in collectd) so that we never get old values and current values intermixed.
* Each `<Value>` can define a custom `PluginName`, allowing you to segment your graphs into different plugin buckets rather than everything being "GenericJMX" or "FastJMX".
* The port can be appended to the hostname using `IncludePortInHostname`. This is very helpful in separating data from multiple JVM instances on the same host without needing to specify an `InstancePrefix` on the `<Connection>`.
* Hostnames are automatically detected from the serviceURL. If the serviceURL is a complex type, like `service:jmx:rmi:///jndi/rmi://hostname:port/jmxrmi`, FastJMX will still properly parse the hostname and port. Of course, if you want to you could still define the hostname property like GenericJMX does...
* FastJMX doesn't require connections be defined after the beans. `<MBean>` (or `<MXBean>`, or just `<Bean>`) and `<Connection>` blocks can come in _any_ order.

### So how much faster is it?

In my testing, I was using the same linux VM to run GenericJMX and FastJMX, connecting to the same remote host. I was sampling 13 values (gc, memory, memory_pools, classes, compilation) from the remote host.

* GenericJMX averaged ~2500ms over 30 reads.
* FastJMX averaged ~120ms over 30 reads.

I added two more hosts, with the same metrics.

* GenericJMX averaged ~7500ms over 30 reads. 2.5 seconds longer than my 5 second interval.
* FastJMX averaged ~260ms over 30 reads.

In further testing, I ended up pulling 99 metrics from 9 different remote servers, over a VPN with an average read cycle taking between 900ms and 1100ms.

## Configuration
Migrate your existing GenericJMX config by:

* Add the path to the fast-jmx jar in JVMARG
* Include `LoadPlugin "org.collectd.FastJMX` in the `<Plugin "java">` block.

Additional Configuration Options:

* Remove the `hostname` from the `<Connection>` blocks. FastJMX can detect it if you don't include it.
* Single-attribute `<Value>` blocks can use the syntax `<Value "attributeName">`. See the `<MBean "classes">` example below.
* Include `PluginName` declarations in a `<Value>` block to change the plugin name it's reported as.
* Use `<MBean>` or `<MXBean>` or `<Bean>`.
* `Composite` and `Table` can be used interchangeably to define a `<Value>`, and can be omitted (defaults to `false`).
* `MaxThreads` can change the default maximum number of threads (512) to allow.
* `CollectInternal` enables internal metrics FastJMX uses to be reported back to Collectd.

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
