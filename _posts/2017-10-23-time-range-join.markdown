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

[INSERT EXAMPLE DATA]

**Stage 2: Partition the data and sort it:** <br/>
Now that we have a combined dataset of `A` and `B`(in terms of `B+` and `B-`), the next step is to partition the data by `id` and sort by `time` within every partition. 

The hash for the same key gives the same result everytime. Based on this idea, hash partition the data  on `id`. The records with the same value of `id` will be part of the same partition<sup>[1]</sup>. Recall that our simple sql had a join on `id`  - `A a LEFT JOIN B b ON (a.id = b.id)`. Roughly speaking, when you partition the data by `id`, it is similar to a join on `id`. For intution, you can think of this partitioning as a `JOIN`  

Sort the data by `time` within every partition. This implicity positions the `a` records in between `b+` and `b-` records, where `time` is in between `start-time` and `end-time`. To get an intution, relate this to the condition in our SQL- ```start-time <= time and time <= end-time``` except nothing is filtered yet. Because, we already hash partitioned by `id`, all values of the same `id` are part of the same partition. Sorting this partition guarentees that our "filter condition" will be effective. 

[INSERT CODE HERE]

[INSERT EXAMPLE DATA]

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
merge(current_record, M) = {
    curr_id = current_record.id 
	//note that curr_points exists only for b+ and  b- records
	//it is null otherwise
    curr_points = current_record.points 
    M.get(curr_id) match {
        case None => 
		//add a new entry in the map
		M += curr_id -> curr_points
        case Some(existing_value) => 
		//sum the value in map with current_record's value
		M += curr_id -> (curr_points + existing_value)
    }
}
```
 


Given a partition of data, it might contain more than one value of `id`.

[INSERT EXAMPLE DATA]



(IN PROGRESS...)
	



# References 
*  Chow Keung, a senior developer in my team, who doesn't have an online presence. 
*  [A stackoverflow question on time-range join][so-join] 

[so-join]: https://stackoverflow.com/q/27138392/1101823 