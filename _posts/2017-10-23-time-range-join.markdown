---
title: Time range join in spark
layout: post
mathjax: true
comments: true
date: '2017-10-23 07:24:01 -0400'
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

# The problem 
Let's say there are two data sets `A` and `B` such that,  
`A` has the fields `{id, time}` and `B` has the fields `{id, start-time, end-time, points}`.  

Find the sum of `points` for a given row in `A` such that `A.id = B.id` and `A.time` is in between `B.start-time` and `B.end-time`. 

Let's make it clearer by adding example data - 

|A|
|id|time|
|--|:----:|
|1|10:00|
|1|10:15|
|2|10:01|


|B|
|id|start-time|end-time|points|
|--|:--------:|:------:|------|
|1|9:30|10:30|10| 
|1|10:01|10:05|20|
|1|10:08|10:20|30|
|1|10:30|10:45|40|
|2|9:30|10:30|50|

Its such a simple question. An equally simple and correct answer would be to write - 
{% highlight sql %}
SELECT 
    a.id, a.time, sum(b.points) 
FROM 
    A a LEFT JOIN B b 
	    ON (a.id = b.id)
WHERE 
    b.start-time <= a.time AND 
    a.time <= b.end-time 
GROUP BY 
    a.id, a.time	
{% endhighlight %}

Based on our answer, the output should be -

|id|time|sum(points)|
|--|:--:|:----------|
|1|10:00|30|// 10 |
|1|10:15|40|// 10 + 30|
|2|10:01|50|// 50|
 
 
[INSERT SVG HERE]
 
 ---
 Here are a few observations: 
 * The join between `A` and `B` is a many-to-many join. It may not seem like much at this scale but it is going to be a nightmare for large datasets. 
 * Not all the records of the join are useful. For instance, the 4th row in `B`, with `40 points` was part of the output of the `JOIN` clause, but it was filtered out in the `WHERE` clause. 
 * There is an overlap in the aggregation(a by product of the many-to-many join). For instance, The first row of `B` with `10 points`, is part of the aggregation of first and second rows in `A`

 Much of these observations do not matter at smaller scales. But when you are dealing with a data set to a scale of billions or trillions of records, it begins to bother you; More so, if there is a skew in that data. In traditional databases, we got by fine by pushing the condition in `WHERE` clause to the `JOIN` clause. Unfortunately for us, because of the way big data systems are designed, non-equi joins cannot be implemented efficiently. 
 
# A solution

Now, you could simply implement the same query as above translated to RDDs or dataframes. As we said before, it is not  scalable enough. It might not even finish depending upon the size of the data. 
...
...
(go on)

#### The algorithm
1. Generalize the datasets and combine them.
2. Partition the data and sort them. 
3. Aggregate per partition.

**Stage 1: Generalize the datasets and combine them:** <br/>
Let's consider a dataset `G` with the fields - `{id, time, row-type, points}`. Datasets `A` and `B` can be generalized into `G` as follows: 

||G|
||id|time|row-type|points|
|--|:--:|:--:|:--:|:----:|
|**A**|id|time|'a'|NULL|
|**B+**|id|start-time|'b+'|points|
|**B-**|id|end-time|'b-'|-1 * points|

* Because we are generalizing the datasets, in order to differentiate between the two, `row-type` is added to `G`.
* We split the `B` dataset into two datasets - `B+` and `B-`. `B+` has `start-time` as `time` in `G`. 
* `B+` dataset has `points` as a positive integer. `B-` has `points` as a negative integer(multiplied by -1)

so,  
$$
G = A  \cup  B_+  \cup  B_-
$$ 

Repeating it more formally - In dataset `G` if the `row-type = b+`, then the value of `time` field is the `start-time` in `B`. The same way, if the `row-type = b-` then the value of `time` in `G` is the `start-time` in `B`. 

For simplicity's sake, I will use the terms `a` records, `b+` records or `b-` records to denote records with `row-type = a`, `row-type=b+` and `row-type=b-` respectively.


|mapped A|
|id|time|row-type|points|
|--|:----:|:-----:|:---:|
|1|10:00|a|null
|1|10:15|a|null
|2|10:01|a|null


|mapped B|
|id|time|row-type|points|
|--|:--------:|:------:|:----:|
|1|9:30|b+|10|
|1|10:30|b-|-10| 
|1|10:01|b+|20|
|1|10:05|b-|-20|
|1|10:08|b+|30|
|1|10:20|b-|-30|
|1|10:30|b+|40|
|1|10:45|b-|-40|
|2|9:30|b+|50|
|2|10:30|b-|-50|



**Stage 2: Partition the data and sort it:** <br/>
Now that we have a combined dataset of `A` and `B`(in terms of `B+` and `B-`), the next step is to partition the data by `id` and sort by `time` and `row-type` within every partition. 

The hash for the same key gives the same result everytime. Based on this idea, hash partition the data on `id`. The records with the same value of `id` will be part of the same partition<sup>[1]</sup>. Recall that our simple sql had a join on `id`  - `A a LEFT JOIN B b ON (a.id = b.id)`. Roughly speaking, when you partition the data by `id`, it is similar to a join on `id`. For intution, you can think of this partitioning as a `JOIN`  

Sort the data by `time` and `row-type` within every partition. This implicity positions the `a` records in between `b+` and `b-` records, where `time` is in between `start-time` and `end-time`. To get an intution, relate this to the condition in our SQL- ```start-time <= time and time <= end-time``` except nothing is filtered yet. Because, we already hash partitioned by `id`, all values of the same `id` are part of the same partition. Sorting this partition guarentees that our "filter condition" will be effective. There is also a sort on `row-type`. This is useful in cases where the `time`s are equal; `row-type` will be a tie-breaker in those situations. Putting it in terms of our query, it decides if `time < end-time` or `time <= end-time`. `row-type` can be useful on real data sets, if you have some meaningful values like `{0,1,2}`. We used `{a, b+, b-}` so that it would be easier to explain. 

There is a nice function in spark that does this for us - `repartitionAndSortWithinPartitions()`

you should really look at [this][spark-repartition-blog], if you want a detailed explanation on its usage, or simply you can look up [spark API][spark-ordered-api]. So, in order to use the function, we need to specify how it partition and sort the data.

First, create case classes with for `G` - 
```scala
case class GKey (id, time, rowType) extends Ordered[GKey]{ 
    def compare(that) = (this.time, this.rowType)
	                        .compare(that.time, that.rowType)
}

case class GValue (points)
```
we want a key-value pair for that function to work, so we created a case class for key with `{id, time, row-type}`. Also, we specified an ordering for the class. Notice that we included only `time` and `row-type`. There is no harm in including `id` in the sorting but we did not, just to illustrate that we need not. If you don't like the idea of specifying ordering on the case class, you can choose to implement ordering implictly. 

Next, we define a hash partitioner to use only `id` from the case class to partition. 
```scala
class IdPartitioner extends HashPartitioner(numPartitions) {
    override def numPartitions = numPartitions
    override def getPartition(key) = {
        val id = key.asInstanceOf[GKey].id
        super.getPartition(id)
    }	
}
```
Now you can use that function - 
```scala
// I'm sure you if you are reading this, you know how to convert 
// the data to an RDD and a key-value pair. 
val gRddKV = gRdd.map(toKeyValuePair)
val partitioner = new IdPartitioner(numPartitions)
val gPartitioned = gRddKV.repartitionAndSortWithinPartitions(partitioner)
```

|Partition - 1|
|id|time|row-type|points|
|--|:----:|:-----:|:---:|
|1|9:30|b+|10|
|1|10:00|a|null
|1|10:01|b+|20|
|1|10:05|b-|-20|
|1|10:08|b+|30|
|1|10:15|a|null
|1|10:20|b-|-30|
|1|10:30|b+|40|
|1|10:30|b-|-10| 
|1|10:45|b-|-40|

|Partition - 2|
|id|time|row-type|points|
|--|:--------:|:------:|:----:|
|2|9:30|b+|50|
|2|10:01|a|null
|2|10:30|b-|-50|


[1] : In reality, pigeon hole principle applies. While it is true that the same key belongs to the same partition, it is also true that other keys might also be part of this partition. 

**Stage 3: Aggregate per partition:** <br/>
The data by this stage is generalized, partitioned and sorted within that partition. 

Now, follow these steps for every partition, separately:<br/>
1. Maintain a map `M` such that `key = id` and `value = points`. Intially, `M` is empty. 
2. Iterate through all the `time` ascending sorted records. 
3. For every record, 
 -  if it is a `b+` or `b-` record, *merge* it with `M`.
 -  if it is an `a` record, take the value of current record's `id` in `M` and attach it to the current record.  
 
**merge.** When I said merge it with `M`, I meant that if `M` already has the `id` from current record, add the `points` from the current record with the value of that `id` in `M`. If the current record is a `b+` record, `points` is added to `M` but when the current record is a `b-` record, `points` is taken away from `M`(because it has a negative value).  
 If `M` doesn't already have the `id` from current record, add a new entry in `M`.

```scala
merge(currentRecord: (String, Long), M: mutable.HashMap[String,Long]) = {
    //extract the id and points from currentRecord
    currId = currentRecord._1 
    //note that currPoints exists only for b+ and  b- records
    //it is null otherwise
    currPoints = currentRecord._2 
    M.get(currId) match {
        case None => 
		//add a new entry in the map
		M += currId -> currPoints
        case Some(existingValue) => 
		//sum the value in map with currentRecord's value
		M += currId -> (currPoints + existingValue)
    }
}
```
The code per partition will look like -
```scala
perPartition(itr: Iterator[(GKey, GValue)]) : Option[(GKey, Long)] = {
//define map M 
M = new mutable.HashMap[String,Long]()
// define an iterator that will aggregate points 
// when it is a b- or b+ record, and extract those aggregated 
// points if it is an a record. 
new Iterator: [Option[(GKey, Long)]] {
    override def hasNext = itr.hasNext
	
    override def next() = {
        val row = itr.next
        val key = row._1
        val value = row._2
       
        // if current record is an a record
        if (key.rowType.equals("a")) {
            //get the value of id from M 
            val aggregatedPoints = M.getOrElse(key.id, 0)
            Some((GKey,aggregatedPoints))
        } 
        else {
            // otherwise it is a b+ or b- record, merge it
            // with the current record
            merge((key.id, value.points), M)  
            //go on until you find an a record
            if (itr.hasNext) {
                this.next
            }
            else {
                None
            }
        }
    }
}
```

That's it! What you get after these 3 stages is an `RDD[Option[(GKey, Long)]]`. What you choose to from here on its upto you. 

One last note before we close, I want to talk a little about why we used an iterator. 
1. `perPartition()` function will be called by RDD's `mapPartitions()`. `mapPartitions` expects an iterator in return. 
2. This is a perfect use case for an iterator. Once, you have final output `(Gkey, Long)`, you don'e need it again for the records to come after that. So, there is no need to keep track of the final output records. You could very well store the output records in a list and return an iterator of that list. But that would be a waste of memory; And you would have to deal with memory issues, JVM tuning, etc.  

|Partition - 1 Intermediate |
|id|time|row-type|points|in M|
|--|:----:|:-----:|:---:|:--:|
|1|9:30|b+|10| M = {1 -> 10}
|1|10:00|a|null| copy 10 points
|1|10:01|b+|20| M = {1 -> 30}
|1|10:05|b-|-20| M = {1 -> 10}
|1|10:08|b+|30| M = {1 -> 40}
|1|10:15|a|null| copy 40 points
|1|10:20|b-|-30| M = {1 -> 10}
|1|10:30|b+|40| M = {1 -> 50}
|1|10:30|b-|-10| M = {1 -> 40}
|1|10:45|b-|-40| M = {1 -> 0}

|Partition - 1 Final|
|id|time|row-type|points|
|--|:----:|:-----:|:---:|
|1|10:00|a|10| 
|1|10:15|a|40|


|Partition - 2 Intermediate|
|id|time|row-type|points|
|--|:--------:|:------:|:----:|
|2|9:30|b+|50| M = {1 -> 50}
|2|10:01|a|null| copy 50 points
|2|10:30|b-|-50| M = {1 -> 0}

|Partition - 2 Final|
|id|time|row-type|points|
|--|:--------:|:------:|:----:|
|2|10:01|a|50|


###Conclusion
What we have illustrated in this post is a very simple example. In many cases, its a lot more complicated. But the if you have understood the base idea, the rest shouldn't be too hard, just make a variant of this solution. 

For example, if the `points` need be be grouped, rather than being aggregated, you could make a change to `merge()` in our code from `(currPoints + existingValue)` to using a `HashSet/LinkedList` data structure that adds `points` to the set/list if it is `b+` and remove from the set/list if it is `b-` record. Or maybe you could implement merge with a function parameter with which you can control how you merge at every step.

So. Sigh! Let me know what you think of this post. Write a comment, ask a question or point out what is wrong. :)

# References and credits
*  Chow Keung, a senior developer in my team, who doesn't have an online presence. 
*  [A stackoverflow question on time-range join][so-join] 
*  [This elaborate blog on how to use repartition and sort within partitions][spark-repartition-blog]

[so-join]: https://stackoverflow.com/q/27138392/1101823 
[spark-repartition-blog]: http://codingjunkie.net/spark-secondary-sort/
[spark-ordered-api]: http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.OrderedRDDFunctions