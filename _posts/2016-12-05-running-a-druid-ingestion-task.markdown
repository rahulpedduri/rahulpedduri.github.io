---
title:  "Running a druid ingestion task"
date:   2016-12-05 00:00:00 -0400
layout: post
mathjax: true
comments: true
categories: spark
---
Druid is an open source data store designed for low-latency exploratory analytics of large amounts of data. It combines a columnar storage layout, a distributed, shared-nothing architecture and an advanced indexing structure to ensure sub second latencies even while exploring billions of records. Druid is a very good for managing **immutable but append-heavy data**.

The druid website offers an in-depth documentation on how to use druid with an example wikiticker data set. The idea of this post is to walk you through the issues I had run into and how I got around it.

I have used Druid 0.9.2 for running these examples. We will use druid using HDFS. We will load the data into druid from files.

# Ready the data
Download the data from this [link][wiki-data]. This link is not mentioned in documentation. It is available in the project's git. Download the wikiticker gz file, extract it and upload it to HDFS. The dataset is in JSON format. Every JSON object in the dataset looks like this -

```json
{
 "time": "2015-09-12T23:59:59.200Z",
 "channel": "#en.wikipedia",
 "cityName": null,
 "comment": "(edited with [[User:ProveIt_GT|ProveIt]])",
 "countryIsoCode": null,
 "countryName": null,
 "isAnonymous": false,
 "isMinor": false,
 "isNew": false,
 "isRobot": false,
 "isUnpatrolled": false,
 "metroCode": null,
 "namespace": "Main",
 "page": "Tom Watson (politician)",
 "regionIsoCode": null,
 "regionName": null,
 "user": "Eva.pascoe",
 "delta": 182,
 "added": 182,
 "deleted": 0
}
```


#Ingest the data
The way to interact with druid is through REST APIs. If you ask me, that is a good way to go because, you can access druid services from any machine in the cluster without actually installing anything on that machine, it just needs to be able to access the URL. Also, there is no language dependency i.e. you are free to use the REST services any language you like. The downside is that, it is a bit painful to write druid queries by hand. The same is easier programmatically though. You can learn about the ingestion formats that druid accepts from [here][batch-ingest].

Let us load the data into druid.

```json
{
  "type" : "index_hadoop",
  "spec" : {
    "ioConfig" : {
      "type" : "hadoop",
      "inputSpec" : {
        "type" : "static",
        "paths" : "wikiticker-2015-09-12-sampled.json"
      }
    },
    "dataSchema" : {
      "dataSource" : "wikiticker",
      "granularitySpec" : {
        "type" : "uniform",
        "segmentGranularity" : "day",
        "queryGranularity" : "none",
        "intervals" : ["2015-09-12/2015-09-13"]
      },
      "parser" : {
        "type" : "hadoopyString",
        "parseSpec" : {
          "format" : "json",
          "dimensionsSpec" : {
            "dimensions" : [
              "channel",
              "cityName",
              "comment",
              "countryIsoCode",
              "countryName",
              "isAnonymous",
              "isMinor",
              "isNew",
              "isRobot",
              "isUnpatrolled",
              "metroCode",
              "namespace",
              "page",
              "regionIsoCode",
              "regionName",
              "user"
            ]
          },
          "timestampSpec" : {
            "format" : "auto",
            "column" : "time"
          }
        }
      },
      "metricsSpec" : [
        {
          "name" : "count",
          "type" : "count"
        },
        {
          "name" : "added",
          "type" : "longSum",
          "fieldName" : "added"
        },
        {
          "name" : "deleted",
          "type" : "longSum",
          "fieldName" : "deleted"
        },
        {
          "name" : "delta",
          "type" : "longSum",
          "fieldName" : "delta"
        },
        {
          "name" : "user_unique",
          "type" : "hyperUnique",
          "fieldName" : "user"
        }
      ]
    },
    "tuningConfig" : {
      "type" : "hadoop",
      "partitionsSpec" : {
        "type" : "hashed",
        "targetPartitionSize" : 5000000
      },
      "jobProperties" : {}
    }
  }
}
```
now, POST this ingestion task to overlord instance

```bash
curl -X 'POST' \
-H 'Content-Type:application/json' \
-d @my-index-task.json \
[druid-overlord-host]:8090/druid/indexer/v1/task
```
You can look at the progress and logs of this index task at coordinator console. The UI is convenient to track progress of the task. But if the configuration setup is not right, you may not see the logs. If you cannot see the logs, you don't know what went wrong when the task fails. Let's take a step back and make changes to the configuration if logs are not showing up. If you are trying out druid for the first time like me, most likely you will need run into the same problems I did.

No log was found for this task. The task may not exist, 
or it may not have begun running yet.
add the following to `conf/druid/middleManager/runtime.properties` on the servers wherever middle manager service is running

```scala
druid.selectors.indexing.serviceName=druid:overlord
druid.indexer.logs.type=hdfs
druid.indexer.logs.directory=/tmp/druid-logs
```
and the following to conf/druid/overlord/runtime.properties on the server where overlord service is running.

```scala
druid.indexer.logs.type=hdfs
druid.indexer.logs.directory=/tmp/druid-logs
```

This will inform druid to dump the logs at `/tmp/druid-logs` on HDFS. And of course, you need to provide the HDFS write access for that location to the user running druid services. For the new configuration to take effect, you need to restart the services. Try POSTing the task again after restarting, this time you should be able to see the logs in the UI and also on HDFS `/tmp/druid-logs`.

It is very likely that your task failed now. Since, you can look at the log it is easy to figure out what went wrong. The first issue that I had after fixing logs is that it cannot find hadoop dependencies.
```
com.metamx.common.ISE: Hadoop dependency didn't exist
```
...
This one is actually easy to figure out. Look at the value of `` in `conf/druid/_common/common.runtime.properties`. This `` value informs druid where the hadoop dependencies are located in. Since, it is configured to the wrong location, it cannot find them and hence can't progress. Hadoop dependencies are located in the home directory of druid i.e. the directory where druid tar is extracted to. Change that value to the correct location and this should go away.

The next one up is this -

Error: 
```
class com.fasterxml.jackson.datatype.guava.deser.HostAndPortDeserializer 
overrides final method 
deserialize.(Lcom/fasterxml/jackson/core/JsonParser;
  Lcom/fasterxml/jackson/databind/DeserializationContext;)
Ljava/lang/Object;
```
This error is due to the fact that hadoop and druid are using conflicting `fasterxml` versions. As a matter of fact there are many other dependencies that druid uses that are already present on hadoop . One common way to solve this issue is to build your own fat jar from druid source. If you do choose to build one, you might want to use this [sbt file][sbt-file]. There is however another way to solve this. Druid provides a way to pass in some properties for the job. Modify the index task to include these new job properties.

```json
"jobProperties": {
"mapreduce.job.classloader": "true",
"mapreduce.job.classloader.system.classes": "-javax.validation.,java.,javax.,org.apache.commons.logging.,org.apache.log4j.,org.apache.hadoop."
}
```
The property `mapreduce.job.classloader` asks hadoop to use a separate classloader for each of hadoop and druid dependencies. i.e. it asks hadoop to load the classes from the jars that are submitted with druid rather than the classes present in system classpath, in case of a conflict.The property `mapreduce.job.classloader.system.classes` is the exclusion rule to the previous property. It defines what classes to be loaded from system classpath.

If you want to explore other ways to deal with this issue, visit their [documentation on github][doc-git]. I actually arrived at the solution from this [google-groups discussion][google-groups]

If druid is actually set up right on your cluster, your index task should run fine after the last one. To me, the mapreduce job got stuck at 100% reduce for quite a long time and all the reduce tasks were failing due to timeouts. When I checked the yarn logs, that when I realized that this last error I had was an access issue. Druid writes the segments to `/druid` by default on HDFS. It was failing because the user running the druid service did not have write access to that directory. Fixed that, and it was all fine.

If you are interested in learning more about the druid architecture and its inner workings, you should read this well written [paper][dr-paper]

# References and credits
* [Druid's official quickstart][druid-quickstart]
* [Paper on druid][dr-paper]



[wiki-data]: https://github.com/druid-io/druid/tree/master/examples/quickstart
[druid-quickstart]: http://druid.io/docs/0.9.2/tutorials/quickstart.html
[dr-paper]: http://static.druid.io/docs/druid.pdf
[doc-git]: https://github.com/druid-io/druid/blob/master/docs/content/operations/other-hadoop.md 
[google-groups]: https://groups.google.com/forum/#!topic/druid-user/UM-Cgj750sY
[sbt-file]:  https://github.com/druid-io/druid/blob/master/docs/content/operations/use_sbt_to_build_fat_jar.md
[batch-ingest]: http://druid.io/docs/0.9.2/ingestion/batch-ingestion.html
