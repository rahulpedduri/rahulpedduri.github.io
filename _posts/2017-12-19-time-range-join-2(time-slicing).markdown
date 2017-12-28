---
title: Time range join in spark - II (time-slicing)
layout: post
mathjax: true
comments: true
date: '2017-12-19 07:24:01 -0400'
---
<style type="text/css" media="screen">
  table {
    border-collapse: collapse;
	margin-bottom: 25px;
}
th, td {
    border-bottom: 1px solid #ddd;
	padding: 2px 15px 2px 15px;
}
tr:hover {background-color: #f5f5f5}
th { background-color: darkgrey;}
tr:nth-child(even) {background-color: #f2f2f2}

</style>

I will continue from our [previous post][tr1] where we solved a time range join problem without actually joining the datasets. If you don't know what I'm talking about, I strongly recommend that you at least read [the problem][problem] we will solve in this post. I will use a different example in this post to illustrate the point, but the problem is still the same. 

In fact, this solution is also very similar to the previous post. There was a drawback in our [previous solution][solution] - **data skew**. [Our previous solution][algorithm] relied on the fact that all the records with the same `id` will be processed in the same partition. What if there are many records in that partition with that same `id` but other partitions have few to no records? Then we are not actually processing the data in parallel. There is a bottleneck on the skewed partition.

Fortunately, we have a work around to that. But, it doesn't remove the skew problem entirely, it gives you customization on how well to distribute the data. 

Lets consider a dataset

[INSERT EXAMPLE DATASET]

#### The algorithm
1. Generalize the datasets and combine them.
2. Partition the data and sort them. 
3. Calculate partial-aggregate and final-map per partition. 
4. Calculate initial-aggregate for every partition. 
5. Merge initial-aggregate with partial-aggregate.

### Stage 1: Generalize the datasets and combine them:
This is similar to the previous post. Except, `G` has an extra field - `time-bucket`. 

Let me first define a function `time-bucket(time, n)` where `time` if the event and `n` is the number of time-buckets; such that, <br/>
For `b1 = time-bucket(t1, n)` and `b2 = time-bucket(t2, n)`, <br/>
if, `t1 < t2` then `b1 <= b2` <br/> 
Also, when `t1 = t2` then `b1 = b2` <br/>

`time-bucket(time, n)` calculates a `bucket-id` for a given `time` by dividing time into `n` buckets; in such a way that, for a `time` less than this `time` will have a smaller or equal `bucket-id`. And for a `time` greater than this time will have a bigger or equal `bucket-id`. 

In human terms, the function divides time into `n` chunks and assigns a monotonically increasing `bucket-id`. For example, 9:30 AM - 10:00 AM may be part of bucket with `bucket-id = 1` and 10:00 AM to 10:30 AM maybe part of a bucket with `bucket-id = 2`. If `n = 24`, we will have 24 time-buckets with a 1-hour maximum time range in each of the bucket.

[INSERT SVG HERE]

||G|
||id|time|bucket|row-type|tie-breaker|points|
|--|:--:|:--:|:--:|:--:|:---:|:----:|
|**A**|id|time|time-bucket(time,n)|'a'|0|NULL|
|**B+**|id|start-time|time-bucket(start-time,n)|'b+'|1|points|
|**B-**|id|end-time|time-bucket(end-time,n)|'b-'|2|-1 \* points|

Combine the three datasets,  
$$
G = A  \cup  B_+  \cup  B_-
$$ 

In dataset `G` if the `row-type = b+`, then the value of `time` field is the `start-time` in `B`. The same way, if the `row-type = b-` then the value of `time` in `G` is the `start-time` in `B`. Also, when `row-type = b+`, the value of `bucket` is `time-bucket(start-time)` in `B` and when `row-type = b-`, the value of `bucket` is `time-bucket(end-time)` in `B` 

For simplicity's sake, I will use the terms `a` records, `b+` records or `b-` records to denote records with `row-type = a`, `row-type = b+` and `row-type = b-` respectively.

Notice that we have also included a `tie-breaker` field which includes simple integers `{0,1,2}` for `a`,`b+` and `b-` respectively. 


[INSERT EXAMPLE DATASET]


### Stage 2: Partition the data and sort it:
The output of stage-1 is `G`, a generalized and combined dataset of the input datasets. In this stage we partition `G` by `{id, bucket}` and then sort by `{time, tie-breaker}` within every partition.  

We originally partitioned the data using `id` only, because of which we ran into skew conditions. Then we decided to time-slice it into buckets. Because our `time-bucket()` accepts the number of buckets to split time into, it gave us flexibility around the distribution of the skew. 

Now, sorting within the partitions. When the partition is sorted by `time` and `tie-breaker`, the `a` records naturally settle down in an order where the time-range condition satisfies. If you followed my last post, we used a `row-type` instead of `tie-breaker` like now. If you recall, we discussed that `row-type` is used as a tie breaker when the `time`s are equal. It determines whether the time-range condition should be a closed bound or an open bound. In this post, I wanted to take this opportunity to use a real `tie-breaker`. And the values assigned to it matter. In our current selection, when the times are equal, the records are sorted in the order of `a`, `b+` and `b-`. I will write another post discussing how the bounds can be tweaked using a `tie-breaker`.

First, case classes with for `G` - 
```scala
case class GKey (id, time, rowType, bucket, tieBreaker) extends Ordered[GKey]{ 
    def compare(that) = (this.time, this.tieBreaker)
	                        .compare(that.time, that.tieBreaker)
}

case class GValue (points)
```

Then, a hash partitioner to use only `id` and `bucket`. 
```scala
class IdBucketPartitioner extends HashPartitioner(numPartitions) {
    override def numPartitions = numPartitions
    override def getPartition(key) = {
        val gKey = key.asInstanceOf[GKey]
        val partKey = (gKey.id, gKey.bucket)
        super.getPartition(partKey)
    }	
}
```
Now partition and sort within partitions - 
```scala
// I'm sure you you can figure out what toKeyValuePair() should have 
val gRddKV = gRdd.map(toKeyValuePair)
val partitioner = new IdBucketPartitioner(numPartitions)
val gPartitioned = gRddKV.repartitionAndSortWithinPartitions(partitioner)
```

[INSERT EXAMPLE DATASET]


### Stage 3: Calculate partial-aggregate and final-map per partition:
The first two stages are very similar to our previous approach. As a matter of fact, stages - 3, 4 and 5 in our current approach are also very similar to stage-3 from our previous approach. Having said that, it is going to get messier from here on.  

Follow these steps for every partition, separately:<br/>
1. Maintain a mutable map `M` such that `key = {id, bucket}` and `value = points`. Initially, `M` is empty. 
2. Maintain a mutable set of keys `K` seen within this partition so far, where `key = {id, bucket}`. Initially `K` is an empty set.
3. Iterate through all the `time` ascending sorted records. 
4. For every record, 
 -  add the key from current record into `K`.
 -  if it is a `b+` or `b-` record, *merge* it with `M`.
 -  if it is an `a` record, take the value of current record's key in `M` and attach it to the current record. Let's call this partial-aggregate `P`
5. After the final data record has been read, 
 -  create `M'`, the *final-map* for this partition, using `M` and `K`. 
 -  *generalize* `P` and `M'` with different `row-type`s.
 -  combine `P` with `M'` into `S`
 
**merge.** The definition of merge has not changed since last approach. If `M` already has the `{id, bucket}`(key) from current record, add the `points` from the current record with the value of that key in `M`. If `M` doesn't already have the key from current record, add a new entry in `M`.

**final-map.** For every key in `K`, look for a value of that key in `M`. If the value exists, take that value, otherwise take `NULL` as the value for that key. The final map `M'` is a copy of the work map `M` after the last record has been read along with the keys that were seen in the current partition but were not part of `M`. `M'` is like a superset of `M`. It should have all the keys from `K`.

**generalize.** The output of this stage is 2 things - the `P`, which is a partial aggregate for a given key and `M'` which is the final status of a given key. Which means both of them have to be part of the same data structure. This is the generalized form we chose.  

||S|
||id|bucket|tie-breaker|row-type|time|points|
|--|:--:|:--:|:--:|:--:|:---:|:----:|
|**P**|id|bucket|1|'p'|time|points|
|**M'**|id|bucket|0|'m'|time|points|

$$
S = P  \cup  M'
$$ 

Notice that the `tie-breaker` prioritizes `M'` over `P`. The reason for that will be evident in the next stages. 

some case classes
```scala
//[COMPLETE CODE]
case class SKey()

case class SValue()
```

```scala
//[COMPLETE CODE]
merge(currentRecord: (String, Long), M: mutable.HashMap[String,Long]) = {
}
```

calculating final map
```scala
//[COMPLETE CODE]
finalMap(K: mutable.HashSet[(String, Int)], M: mutable.HashMap[String,Long])
     = {
}
```

The code per partition will look like -
```scala
//[COMPLETE CODE]
partialAggregatePerPartition(itr: Iterator[(GKey, GValue)]) : Option[(GKey, Long)] = {
}
```

[INSERT EXAMPLE DATASET]


### stage 4: Calculate initial-aggregate for every partition


### Stage 5: Merge initial-aggregate with partial-aggregate



### Conclusion


# References and credits
*  [Chow Keung(CK)][ck], a senior developer in my team. 
*  [A stackoverflow question on time-range join][so-join] 
*  [This elaborate blog on how to use repartition and sort within partitions][spark-repartition-blog]


[ck]: https://www.linkedin.com/in/chowck/
[tr1]: {{ site.baseurl }}{% post_url 2017-10-23-time-range-join  %}
[problem]: {{ site.baseurl }}{% post_url 2017-10-23-time-range-join  %}#the-problem 
[solution]: {{ site.baseurl }}{% post_url 2017-10-23-time-range-join  %}#a-solution
[algorithm]: {{ site.baseurl }}{% post_url 2017-10-23-time-range-join  %}#the-algorithm
[so-join]: https://stackoverflow.com/q/27138392/1101823 
[spark-repartition-blog]: http://codingjunkie.net/spark-secondary-sort/
[spark-ordered-api]: http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.OrderedRDDFunctions