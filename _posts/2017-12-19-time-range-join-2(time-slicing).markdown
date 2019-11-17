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

[TODO: INSERT EXAMPLE DATASET]

#### The algorithm
1. Generalize the datasets and combine them.
2. Partition the data and sort them. 
3. Calculate partial-aggregate and final-map per partition. 
4. Filter, partition and sort within partitions.
5. Calculate initial-aggregate for every partition. 
6. Merge initial-aggregate with partial-aggregate.

### Stage 1: Generalize the datasets and combine them:
This is similar to the previous post. Except, `G` has an extra field - `time-bucket`. 

Let me first define a function `time-bucket(time, n)` where `time` if the event and `n` is the number of time-buckets; such that, <br/>
For `b1 = time-bucket(t1, n)` and `b2 = time-bucket(t2, n)`, <br/>
if, `t1 < t2` then `b1 <= b2` <br/> 
Also, when `t1 = t2` then `b1 = b2` <br/>

`time-bucket(time, n)` calculates a `bucket-id` for a given `time` by dividing time into `n` buckets; in such a way that, for a `time` less than this `time` will have a smaller or equal `bucket-id`. And for a `time` greater than this time will have a bigger or equal `bucket-id`. 

In human terms, the function divides time into `n` chunks and assigns a monotonically increasing `bucket-id`. For example, 9:30 AM - 10:00 AM may be part of bucket with `bucket-id = 1` and 10:00 AM to 10:30 AM maybe part of a bucket with `bucket-id = 2`. If `n = 24`, we will have 24 time-buckets with a 1-hour maximum time range in each of the bucket.

[TODO: INSERT SVG HERE]

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


[TODO: INSERT EXAMPLE DATASET]


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

[TODO: INSERT EXAMPLE DATASET]


### Stage 3: Calculate partial-aggregate and final-map per partition:
The first two stages are very similar to our previous approach. As a matter of fact, stages - 3, 4, 5 and 6 in our current approach are also similar to stage-3 from our previous approach but it gets more complicated. It is going to get messier from here on.  

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
|**M'**|id|bucket|0|'m'|null|points|

Combining them,     
$$
S = P  \cup  M'
$$ 

Notice that the `tie-breaker` prioritizes `M'` over `P`. There is no use for a `tie-breaker` until stage-6, however. 

Also, just like what we did in stage-1, I will use the terms `p` records and `m` records to denote the records with `row-type = p` and `row-type = m` respectively.

some case classes
```scala
//[TODO: COMPLETE CODE]
case class SKey()

case class SValue()
```

```scala
//[TODO: COMPLETE CODE]
merge(currentRecord: (String, Long), M: mutable.HashMap[String,Long]) = {
}
```

calculating final map
```scala
//[TODO: COMPLETE CODE]
finalMap(K: mutable.HashSet[(String, Int)], M: mutable.HashMap[String,Long])
     = {
}
```

The code per partition will look like -
```scala
//[TODO: COMPLETE CODE]
partialAggregatePerPartition(itr: Iterator[(GKey, GValue)]) : Option[(GKey, Long)] = {
}
```

[TODO: INSERT EXAMPLE DATASET]


### stage 4: Filter, partition and sort within partitions 
The output of stage-3 is a dataset `S`, which can be separated into `p` records and `m` records based on the `row-type` field. The next stage of this algorithm is based on `m` records only. 

In this stage, we follow the following three steps: <br/> 
1. Filter `S` for `m` records. 
2. Partition the `m` records by `{id}`. This will guarantee that all records with the same `id` are part of the same partition. 
3. Sort within every partition by `{bucket}`. 

The code for the partitioner -
```scala
//[TODO: COMPLETE CODE]

```

Repartition the data and sort within partitions -
```scala
//sort order is already defined in <> case class definition
//[TODO: COMPLETE CODE]
repartitionAndSortWithinPartitions()
```


[TODO: INSERT EXAMPLE DATASET]


### stage 5: Calculate initial-aggregate for every partition

We already know that a `p` record has a partial aggregate based on `b+`/`b-` records seen within that time bucket. In order to complete the partial aggregate, it has to look at `b+`/`b-` records for all the time buckets before the current bucket; The partial aggregate that was calculated has no idea about the time ranges and associated `points` for anything outside the current time bucket.

If you think about it, in order to complete `p`, we need not look at each and every `b+`/`b-` record before the current time bucket, instead an aggregate of those until the start of the current time bucket will still complete `p`. Besides looking at each and every one of those records will be an overhead. 


[TODO: INSERT SVG HERE] 

In this stage, we derive `W`, the initial-aggregate - an aggregate of `b+`/`b-` records until the start of every time bucket for every `id`, using `M'`.

To derive `W`, we follow the following steps for every partition of data - 
1. Maintain a mutable work map `N` such that `key = {id}` and `value = {points}`.
2. Let's say the record is `R[k,v]`, a key-value pair `r[k]` as the key in the tuple and `r[v]` as the value. 
3. Find the value of `r[k]` in `N`. Let's say that value is `r[n]`. If there is no key, populate `null` and ignore the next step. 
4. Return the record `R[k,n]` with `r[k]` as the key and `r[n]` as the value. This record will be part of `W`. 
5. Update `N` with `R[k,v]`. If `r[k]` exists in `N`, sum the value of that key with `r[v]`. If it doesn't exist, add a new entry in `N`.

The order of the steps 3, 4 and 5 is important. Because `W` is the aggregate of all time buckets before the current one. It should not include the current time bucket. 

To summarize,
  
$$
 W_n = \sum_{i=1}^n m_i
$$
 <br/>where W<sub>n</sub> is the initial-aggregate at the start of time bucket `n`
 
$$
\begin{align}
 W_1 & = empty \\
 W_2 & = m_1 \\ 
 W_3 & = m_1 \oplus m_2 \\
 W_4 & = m_1 \oplus m_2 \oplus m_3 \\
 & ... \\ 
 W_n & = m_1 \oplus m_2 \oplus ... \oplus m_{n-1}
\end{align}
$$

```scala
//[TODO: COMPLETE CODE]

```

[TODO: INSERT EXAMPLE DATASET]

### Stage 6: Merge initial-aggregate with partial-aggregate
At the end of stage-3, we had dataset `S` with `p` and `m` records. In stage-4 and stage-5, we used `m` records only. The key for `m` records did not change, only the value part of the record changed. In other words, `W` still has the field `row-type = m`. In order to avoid confusion, lets change the `row-type` in `W` to `w` and keep everything else same. 

We need to create `S'` from `S` such that 
$$
S = P  \cup  W
$$

||S'|
||id|bucket|tie-breaker|row-type|time|points|
|--|:--:|:--:|:--:|:--:|:---:|:----:|
|**P**|id|bucket|1|'p'|time|points|
|**W**|id|bucket|0|'w'|null|points|

Take note of the `tie-breaker` again. This order will guarantee that `w` records always show up before `p` records for the same `id` within the same `bucket`. 

Once again, 
1. Partition `S'` by `{id, bucket}`. 
2. Sort within every partition by `{id, bucket, tie-breaker}`

Now follow these steps within every partition: 
1. Maintain a mutable map `O` such that `key = {id, bucket}` and `value = points`. Initially, `O` is empty. 
2. For every record, 
 -  if it is a `w` record, update `O` with it.
 -  if it is a `p` record, take the value of current record's key in `O` and sum it with the value of current record. 

 
```scala
//[TODO: COMPLETE CODE]

```

[TODO: INSERT EXAMPLE DATASET]

### Conclusion
Although this algorithm is complicated, it gives more control over a skew. For example, if the data is highly skewed, I would simply reduce the time window for a time bucket, which distributes the skewed key into multiple buckets. When we implemented this algorithm to solve our time-range join, the performance was phenomenal. There is a downside however - it has a minimum performance overhead because of the many stage approach. We chose to use both approaches to a time-range join. 

In this algorithm, I tried to simplify as much as possible. We used a simple aggregation in a time range problem. In another post, I will demonstrate how to get all records within 'n' seconds of a record - a different time range join problem. 

Comments/suggestions/criticism, welcome!


# References and credits
*  [Chow Keung(CK)][ck], a principal developer in my team. 
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