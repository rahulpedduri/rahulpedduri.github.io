---
title: Time range join in spark
layout: post
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
|1|10:00|10:05|20|
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
|1|10:00|30|// 10 + 20|
|1|10:15|40|// 10 + 30|
|2|10:01|50|// 50|
 
 
 ---
 Here are a few observations: 
 * The join between `A` and `B` is a many-to-many join. It may not seem like much at this scale but it is going to be a nightmare for large datasets. 
 * Not all the records of a join are useful. For instance, the 4th row in `B`, with `40 points` was part of the output of the `JOIN` clause, but it was filtered out in the `WHERE` clause. 
 * There is an overlap in the aggregation(a by product of the many-to-many join). For instance, The first row of `B` with `10 points`, is part of the aggregation of first and second rows in `A`

 Much of these observations do not matter at smaller scales. But when you are dealing with a data set to a scale of billions or trillions of records, it begins to bother you; More so, if there is a skew in that data. In traditional databases, we got by fine by pushing the condition in `WHERE` clause to the `JOIN` clause. Unfortunately for us, because of the way big data systems are designed, non-equi joins cannot be implemented efficiently. 
 
# A solution

Now, you could simply implement the same query as above translated to RDDs or dataframes. As we said before, it is not  scalable enough. It might not even finish depending upon the size of the data. 
...
...

#### The algorithm
1. Generalize the datasets and combine them.
2. Partition the data and sort them. 
3. Aggregate per partition.

**Step 1: Generalize the datasets and combine them:** <br/>
Let's consider a dataset `G` with the fields - `{id, time, row-type, points}`. Datasets `A` and `B` can be generalized into `G` as follows: 

||G|
||id|time|row-type|points|
|--|:--:|:--:|:--:|:----:|
|**A**|id|time|'a'|NULL|
|**B+**|id|start-time|'b'|points|
|**B-**|id|end-time|'b'|-1 * points|

* Because we are generalizing the datasets, in order to differentiate between the two, `row-type` is added to `G`.
* We split the `B` dataset into two datasets - `B+` and `B-`. `B+` has `start-time` as `time` in `G`.  
(in progress...)
	



# References 
*  Chow Keung, a senior developer in my team, who doesn't have an online presence. 
*  [A stackoverflow question on time-range join][so-join] 

[so-join]: https://stackoverflow.com/q/27138392/1101823 