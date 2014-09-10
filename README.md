# FastJMX - A low-latency JMX plugin for collectd

The default GenericJMX plugin from collectd is great for basic collection of small numbers of metrics, but if you need to collect many metrics from one 
or more hosts, the latency to read the metrics can quickly exceed your interval time, and that's no fun. If you want to remotely collect metrics from
multiple hosts you can forget about having short intervals, and some of the configuration settings aren't exactly obvious. 
Example: What do you mean I have to include the hostname? I gave you the serviceUrl!

## Introducing FastJMX - Async ~~all~~ the things!

FastJMX does things differently than the GenericJMX plugin, but it does it in a manner that's configuration-compatible with the original plugin.
(You read that right. There's just a few small tweaks to an existing configuration and FastJMX will take over)

* FastJMX discovers all the matching beans when it first connects, then sets up listeners to the remote server so we get callbacks when any beans are added or removed from the server. This lets us identify all the permutations of the beans we need to read outside of the `read()` loop.
* Reconnections are attempted with increasing backoff sleep durations. Again, outside of the read loop, so no other servers are blocked.
* Each attribute read from an mbean is it's own potential thread. The JDK 1.5 Concurrent packages are used to pool threads, inflict interval timeouts on the read cycle, and to make sure the queue is clear at the end of each `read()` invocation eliminating backlogged metric reporting.
* Each `read()` cycle is timeslot protected (synched to the interval configured in collectd) so that we never get old values and current values intermixed.
* Each `<Value>` can define a custom `PluginName`, allowing you to segment your graphs into different plugin buckets rather than everything being "GenericJMX" or "FastJMX".
* The port can be appended to the hostname using `IncludePortInHostname`. This is very helpful in separating data from multiple JVM instances on the same host without needing to specify an `InstancePrefix` on the `<Connection>`.
* Hostnames are automatically detected from the serviceURL. If the serviceURL is a complex type, like `service:jmx:rmi:///jndi/rmi://hostname:port/jmxrmi`, FastJMX will still properly parse the hostname and port. Of course, if you want to you could still define the hostname property like GenericJMX does...

### So how much faster is it?

In my testing, I was using the same linux VM to run GenericJMX and FastJMX, connecting to the same remote host. I was sampling 13 values (gc, memory, memory_pools, classes, compilation) from the remote host.

* GenericJMX averaged ~2500ms over 30 reads.
* FastJMX averaged ~120ms over 30 reads.

I added two more hosts, with the same metrics.

* GenericJMX averaged ~7500ms over 30 reads. 2.5 seconds longer than my 5 second interval.
* FastJMX averaged ~260ms over 30 reads.

In further testing, I ended up pulling 99 metrics from 9 different remote servers, over a VPN with an average read cycle taking between 900ms and 1100ms.

