---
title:  "Bulk load into HBase"
date:   2016-12-16 00:00:00 -0400
layout: post
mathjax: true
comments: true
categories: spark
---
We had a use case where we had to dump events from Kafka stream into HBase. We were using spark to capture the events per second and store them in Hbase. The problem was, we were getting at least 100,000 events/second and our target was way more than that. Hbase was taking its own sweet time doing a put per event. It was obvious that we had to consider bulk loading the data into Hbase. I will not go into why bulk loading is faster than normal loading of data. You can read about it in this [cloudera blog][cl-blog].

That blog outlines three steps in Bulk load -

1. Extract the data into HDFS
2. convert the data into Hfiles, a format that Hbase stores data in
3. Inform Hbase about the location of these Hfiles

What I would write about is how to convert those three steps into code that works. I am using spark in the examples

# Extract the data into HDFS

This is actually the easiest step among three and the one that lies completely independent of Hbase. In our case we had to pull data from kafka and store it in an RDD for that time window. At the end of this step you should have the data to be loaded into Hbase, ready to be processed on HDFS. Just for the sake of this discussion, I will use a dummy dataset.

```scala
//a simple data set of strings
val rdd = sc.parallelize(List("a", "b", "c", "d"))
```

# Convert the data into HFiles

You can actually write a mapreduce program to complete this step. This should give you some pointers to get started.

This is the longest step among the three. We can break down this step into sub tasks -

1. Partition the data to match Hbase region servers
2. Map the data into a Hbase writable format
3. Ready the configuration for Hbase
4. Save the data in HFiles


## Partition the data to match Hbase region servers
We would want to partition the data to match the partitioning of Hbase region servers. Since, we are using spark RDDs, it partitions the data set based on the hashcode of the entire record of that dataset. Hbase region servers on the other hand partition data based on the row key that is associated with that row. In fact, you can consider this task optional but it will be helpful later.

```scala
val saltMod = 2 //for salting the key with
//zipWithUniqueId attaches a unique ID to every record of the rdd
//repartitionAndSortWithinPartitions will partition the data
val partitioned = rdd.zipWithUniqueId()
    .map({ case (v, k) =(Math.abs(k.hashCode()) % saltMod, v) })
    .repartitionAndSortWithinPartitions(new HashPartitioner(saltMod))
```

What we did is, we simply attached a unique ID to every record of the RDD and we made a new key out of that ID(salting). Based on this new key, we re-partitioned the RDD.


## Map the data into a Hbase writable format
When I say Hbase writable format, I mean a subclass of org.apache.hadoop.io.Writable interface. In our example, we will convert to `org.apache.hadoop.hbase.KeyValue` class.

Of course, before we think about bulk loading the data, we need to have an Hbase table ready. I brought that up just now because, we will use the table name and column family names in the code.

```scala
//lets get declarations out of the way
val tableName = "dummy_table"
val columnFamilyName = "dummy_cf"
val qualifierName = "dummy_message"

//lets convert the data
val transformed = partitioned.map({
     case (k, v) =&amp;amp;amp;amp;amp;amp;gt;
        val key = Bytes.toBytes(v.hashCode() )
        val kv = new KeyValue(key,
                         Bytes.toBytes(columnFamilyName),
                         Bytes.toBytes(qualifierName),
                         Bytes.toBytes(v))
        (new ImmutableBytesWritable(key), kv)
 })
```

## Ready the configuration for Hbase
Most of the configuration we used here are pretty standard except for fs.permissions.umask-mode property. This property defines what the permissions of the HFiles created at the end of this step is going to have. Setting this property with 000 makes sure that the HFiles have 777 permission set. If this is not desirable, you need to choose an appropriate umask value. Permissions are important in this context because, this spark job is run as you(user) and so the HFiles created at the end of this step will be owned by you. But, when you inform HBase of these HFile locations, Hbase will run bulk load operations using its own user (hbase). If that user doesn't have permissions to access the HFiles, the job will fail. And yes, the HFiles can be deleted once Hbase completes its operation.

```scala
 val conf = HBaseConfiguration.create()
 conf.set(TableOutputFormat.OUTPUT_TABLE, "dummy_table")
 conf.set( "mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat")
 conf.set("mapreduce.job.output.key.class", "org.apache.hadoop.hbase.io.ImmutableBytesWritable")
 conf.set("mapreduce.job.output.value.class", "org.apache.hadoop.hbase.KeyValue")
 conf.set("hbase.zookeeper.quorum", "localhost:2181")
 conf.set("zookeeper.session.timeout", 10000)
 conf.set("zookeeper.recovery.retry", 3)
 conf.set("hbase.zookeeper.property.clientport", 2181)
 conf.set("fs.permissions.umask-mode", "000")
```


## Save the data in HFiles
While saving the HFile, you need to provide the class used as key, value and the output format. Hbase considers everything in terms of Bytes. There has to be a way to track what that bulk of byte was originally, we provide classes for that reason.

```scala
val path = "/tmp/hbase-bulk-load"
transformed.saveAsNewAPIHadoopFile(path,
    classOf[ImmutableBytesWritable],
    classOf[KeyValue],
    classOf[HFileOutputFormat2],
    conf)
```


# Inform Hbase about the HFiles
Using the same configuration as before, we first create a HFile loader. And next, we get an instance of the Hbase table that we want to load the data into. Now, we can call the bulk load operation.

```scala
val loader = new LoadIncrementalHFiles(conf)
val connection = ConnectionFactory.createConnection(conf)
val table = connection.getTable(TableName.valueOf(tableName))
                      .asInstanceOf[HTable]
try {
    loader.doBulkLoad(new Path(path), table)
} finally {
    connection.close()
    table.close()
}
```


# References and credits
* [cloudera blog on hbase bulk load][cl-blog]
* [opencore blog on hbase bulk load][opencore-blog]


[cl-blog]: http://blog.cloudera.com/blog/2013/09/how-to-use-hbase-bulk-loading-and-why/
[opencore-blog]: http://www.opencore.com/blog/2016/10/efficient-bulk-load-of-hbase-using-spark/