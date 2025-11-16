# FastJMX - Low-Latency JMX Plugin for Collectd

[![Build Status](https://travis-ci.org/egineering-llc/collectd-fast-jmx.svg?branch=master)](https://travis-ci.org/egineering-llc/collectd-fast-jmx)
[![Maven Central](https://img.shields.io/badge/maven--central-1.0.0-blue.svg)](https://oss.sonatype.org/content/repositories/releases/com/e-gineering/collectd-fast-jmx/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

A high-performance, drop-in replacement for collectd's GenericJMX plugin that dramatically reduces metric collection latency through asynchronous operations and intelligent connection management.

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Performance](#performance)
- [Installation](#installation)
  - [Maven Dependency](#maven-dependency)
  - [Migration from GenericJMX](#migration-from-genericjmx)
- [Configuration](#configuration)
  - [Basic Configuration](#basic-configuration)
  - [Configuration Options](#configuration-options)
  - [Example Configuration](#example-configuration)
- [Internal Metrics](#internal-metrics)
- [Platform-Specific Guides](#platform-specific-guides)
  - [JBoss EAP 6.x / AS 7.x](#jboss-eap-6x--as-7x)
- [Debugging & Troubleshooting](#debugging--troubleshooting)
- [Building from Source](#building-from-source)
- [Contributing](#contributing)
- [License](#license)

## Overview

The default GenericJMX plugin from collectd is great for basic collection of small numbers of metrics, but if you need to collect many metrics from one or more hosts, the latency to read the metrics can quickly exceed your interval time. Remote collection from multiple hosts makes the problem even worse, and some configuration settings aren't exactly intuitive.

**FastJMX solves these problems** while remaining configuration-compatible with the original GenericJMX plugin. With just a few small tweaks to your existing configuration, FastJMX will take over and deliver dramatically improved performance.

## Key Features

- **Asynchronous Bean Discovery**: Discovers matching beans at connection time and sets up listeners for bean additions/removals, keeping this work outside the read() loop
- **Smart Reconnection Logic**: Reconnections are attempted with increasing backoff, outside the read loop, so healthy connections continue collecting metrics
- **Concurrent Attribute Reading**: Each MBean attribute read is its own potential thread using JDK 1.5 Concurrent packages
- **Interval Timeout Protection**: Read cycles have inflicted interval timeouts with queue clearing to eliminate backlogged metric reporting
- **Timeslot Synchronization**: Each read() cycle is synchronized to the configured interval, preventing intermixed old and current values
- **Custom Plugin Names**: Each `<Value>` can define a custom `PluginName` for better metric organization
- **Automatic Hostname Detection**: Parses hostnames from complex service URLs like `service:jmx:rmi:///jndi/rmi://hostname:port/jmxrmi`
- **Port-in-Hostname Support**: Optional `IncludePortInHostname` for separating multiple JVM instances on the same host
- **Flexible Bean Ordering**: `<MBean>`, `<MXBean>`, and `<Connection>` blocks can appear in any order
- **TabularData Support**: Can traverse `TabularData` to extract `CompositeData` values as tables or independent values

## Performance

In real-world collection scenarios, large-volume remote collections from multiple hosts over a VPN improved from **~2500ms to ~120ms** - over 20x faster!

To see exactly what FastJMX is doing, add `CollectInternal true` to the plugin configuration to dispatch internal metrics (success, failure, error, latency, thread pool size) to collectd.

## Installation

### Important Note: Version 1.0.0 Package Changes

In the process of transferring this project to E-gineering, LLC, the Maven coordinates changed to enable publishing to Maven Central.

**Package name changed from:** `org.collectd.FastJMX`
**Package name changed to:** `com.e_gineering.collectd.FastJMX`

### Maven Dependency

Pre-built artifacts are available from [Maven Central](https://oss.sonatype.org/content/repositories/releases/com/e-gineering/collectd-fast-jmx/):

```xml
<dependency>
    <groupId>com.e-gineering</groupId>
    <artifactId>collectd-fast-jmx</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Migration from GenericJMX

Migrating from GenericJMX is straightforward:

1. Add the path to the fast-jmx JAR in JVMARG
2. Change `LoadPlugin "GenericJMX"` to `LoadPlugin "com.e_gineering.collectd.FastJMX"`
3. Change `<Plugin "GenericJMX">` to `<Plugin "FastJMX">`
4. Optionally remove `hostname` from `<Connection>` blocks (FastJMX auto-detects from the JMX URI)

## Configuration

### Basic Configuration

```xml
LoadPlugin java
<Plugin "java">
  JVMARG "-Djava.class.path=/path/to/collectd-api.jar:/path/to/collectd-fast-jmx.jar"

  LoadPlugin "com.e_gineering.collectd.FastJMX"

  <Plugin "FastJMX">
    <!-- Your MBean and Connection definitions here -->
  </Plugin>
</Plugin>
```

### Configuration Options

FastJMX supports all GenericJMX options plus several enhancements:

#### Global Plugin Options

- **`MaxThreads`** - Maximum number of threads to allow (default: 512)
- **`CollectInternal`** - Enable internal metrics collection (default: false)
- **`LogLevel`** - Java log level: SEVERE, WARNING, INFO, FINE, FINER, FINEST (default: INFO)
- **`ForceLoggingTo`** - Force all Java logging to specific collectd level (default: none)

#### Connection Options

- **`ServiceURL`** - JMX service URL (required)
- **`Hostname`** - Override auto-detected hostname (optional)
- **`IncludePortInHostname`** - Append port to hostname (default: false)
- **`Username`** - JMX authentication username (optional)
- **`Password`** - JMX authentication password (optional)
- **`Synchronous`** - Force synchronous operation (default: false, auto-enabled for JBoss Remoting)
- **`TTL`** - Force reconnect after N seconds (useful for servers that don't maintain MBeans after redeployments)
- **`Collect`** - Reference to MBean definition(s) to collect

#### Value Options

- **`Type`** - Collectd type for the metric (required)
- **`InstancePrefix`** - Prefix for the type instance (optional)
- **`InstanceFrom`** - Use MBean property as instance name (optional)
- **`PluginName`** - Override plugin name for this metric (optional, allows grouping different MBeans)
- **`Composite`** / **`Table`** - Read as composite/table data (default: false, both terms are interchangeable)

#### Simplified Syntax

- **Single-attribute Values**: Use `<Value "attributeName">` for brevity
- **Composite/Table**: `Composite` and `Table` keywords can be used interchangeably
- **Bean Tags**: Use `<MBean>`, `<MXBean>`, or `<Bean>` - they're all equivalent

### Example Configuration

```xml
LoadPlugin java
<Plugin "java">
  JVMARG "-Djava.class.path=/path/to/collectd-api.jar:/path/to/collectd-fast-jmx.jar"

  LoadPlugin "com.e_gineering.collectd.FastJMX"

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

    # Time spent by the JVM compiling or optimizing
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

      # Reads the Par Eden Space data as a composite table
      <Value "LastGcInfo.memoryUsageAfterGc.Par Eden Space">
        Type "java_memory"
        Composite true
        InstancePrefix "pool-eden-after"
        PluginName "JVM"
      </Value>

      # Reads only the "used" portion of the Par Eden Space
      <Value "LastGcInfo.memoryUsageAfterGc.Par Eden Space.used">
        Type "java_memory"
        InstancePrefix "pool-eden-after-used"
        PluginName "JVM"
      </Value>
    </MBean>

    # Memory usage by memory pool
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
      # Force the connection to reset every 4 hours
      TTL 14400
    </Connection>

  </Plugin>
</Plugin>
```

## Internal Metrics

FastJMX collects internal metrics to estimate efficient pool size and monitor performance. Enable with `CollectInternal true` in the plugin configuration.

### Required types.db Definitions

Add these to your `types.db` file:

```
fastjmx_cycle      value:GAUGE:0:U
fastjmx_latency    value:GAUGE:0:U
```

### Collection3 Graph Configurations

Useful graph configurations for visualizing FastJMX metrics:

```xml
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

## Platform-Specific Guides

### JBoss EAP 6.x / AS 7.x

The JBoss Remoting JMX provider has been tested with EAP 6.x and should work with AS 7.x. FastJMX includes workarounds for some JBoss-specific behaviors, and appropriate bugs/feature requests have been filed with JBoss.

#### Classpath Configuration

Example JVMArg for JBoss EAP 6.1:

```xml
<Plugin java>
  JVMArg "-Djava.class.path=/usr/share/collectd/java/collectd-api.jar:/usr/lib/jvm/java-7-oracle/lib/jconsole.jar:/usr/lib/jvm/java-7-oracle/lib/tools.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/remoting-jmx/main/remoting-jmx-1.1.0.Final-redhat-1.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/remoting3/main/jboss-remoting-3.2.16.GA-redhat-1.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/logging/main/jboss-logging-3.1.2.GA-redhat-1.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/xnio/main/xnio-api-3.0.7.GA-redhat-1.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/xnio/nio/main/xnio-nio-3.0.7.GA-redhat-1.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/sasl/main/jboss-sasl-1.0.3.Final-redhat-1.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/marshalling/main/jboss-marshalling-1.3.18.GA-redhat-1.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/marshalling/river/main/jboss-marshalling-river-1.3.18.GA-redhat-1.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/as/cli/main/jboss-as-cli-7.2.1.Final-redhat-10.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/staxmapper/main/staxmapper-1.1.0.Final-redhat-2.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/as/protocol/main/jboss-as-protocol-7.2.1.Final-redhat-10.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/dmr/main/jboss-dmr-1.1.6.Final-redhat-1.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/as/controller-client/main/jboss-as-controller-client-7.2.1.Final-redhat-10.jar:/opt/appserver/jboss-eap-6.1/modules/system/layers/base/org/jboss/threads/main/jboss-threads-2.1.0.Final-redhat-1.jar:/usr/share/collectd/java/collectd-fast-jmx-1.0.0.jar"
  LoadPlugin "com.e_gineering.collectd.FastJMX"

  ...

</Plugin>
```

#### Administrator Connection

To connect as an administrator (no JBoss configuration changes needed):

```xml
<Connection>
  ServiceURL "service:jmx:remoting-jmx://yourhostname:9999"
  Username "admin"
  Password "yourAdminPassword"
  TTL 300
  IncludePortInHostname false
  Collect "classes"
  ...
</Connection>
```

#### Application User Connection

To connect as a normal application user over the remoting port, update your domain (or standalone) configuration with `use-management-endpoint="false"`:

```xml
<subsystem xmlns="urn:jboss:domain:jmx:1.2">
  <expose-resolved-model/>
  <expose-expression-model/>
  <remoting-connector use-management-endpoint="false"/>
</subsystem>
```

This changes the port from 9999 (management) to 4447 (remoting) and requires an *application* user rather than an *administration* user.

Add the application user:

```bash
$JBOSS_HOME/bin/add-user.sh --silent -a --user jmx --password yourpassword
```

Connection configuration:

```xml
<Connection>
  ServiceURL "service:jmx:remoting-jmx://yourhostname:4447"
  Username "jmx"
  Password "yourpassword"
  TTL 300
  IncludePortInHostname false
  Collect "classes"
  ...
</Connection>
```

**WARNING**: This unprivileged user will be able to invoke MBeans via JMX. Ensure appropriate security measures are in place.

## Debugging & Troubleshooting

FastJMX provides additional configuration options for troubleshooting:

### Log Level Configuration

- **`LogLevel`** - Sets the plugin's internal Java log level (default: INFO)
  - Valid values: SEVERE, WARNING, INFO, FINE, FINER, FINEST
  - Messages at this level or higher are logged to collectd at corresponding levels

- **`ForceLoggingTo`** - Overrides normal log level mapping and forces all Java log output to a specific collectd level
  - Useful when you want detailed FastJMX logs without changing collectd's global log level

### Examples

**Example 1**: Get INFO-level logs from FastJMX when collectd is set to WARNING:

```xml
<Plugin "FastJMX">
   LogLevel "INFO"
   ForceLoggingTo "WARNING"
   ...
</Plugin>
```

**Example 2**: Enable verbose FINE logging from FastJMX:

```xml
<Plugin "FastJMX">
   LogLevel "FINE"
   ForceLoggingTo "WARNING"
   ...
</Plugin>
```

This configuration writes all Java log messages at FINE level or higher as collectd WARNING messages, giving you fine-grained control over plugin verbosity.

## Building from Source

### Prerequisites

- Java JDK 1.5 or higher
- Maven 3.0.1 or higher

### Build Steps

```bash
# Clone the repository
git clone https://github.com/egineering-llc/collectd-fast-jmx.git
cd collectd-fast-jmx

# Build the project
mvn clean package

# The JAR will be created in target/collectd-fast-jmx-1.0.0.jar
```

### Building with GPG Signing (for releases)

```bash
mvn clean package -P eg.oss
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

### Development

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Issue Reporting

Please report issues at: https://github.com/egineering-llc/collectd-fast-jmx/issues

## License

Copyright 2014 E-Gineering, LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

---

**Maintained by**: E-gineering, LLC
**Website**: http://www.e-gineering.com
**Issues**: https://github.com/egineering-llc/collectd-fast-jmx/issues
