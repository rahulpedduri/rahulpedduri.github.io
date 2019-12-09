---
title: The union sort method
layout: post
mathjax: true
comments: true
date: '2019-11-16'
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

Time range joins are a real menance in distributed computing, but a lot of things have changed in last few years. Spark has many time range optimizations in place. And databricks platform provides a way to hint about a time range join so that it can optimize the join. Many organizations have their own way to deal with time range joins.

Taking the prologue of my [first][tr1] and [second][tr2] posts about time range joins, I want to introduce the concept of a union sort method to solve those problems. As a matter of fact I want to generalize the use of this powerful method. 

### Why care? 
Last week a collegue asked me for help in solving a specific time range problem. He has a very large dataset that we wants to self join and find all pairs of overlapping intervals. The problem is that the key used in the join has over a ~80 million records. Now, 



# References and credits
*  [Chow Keung(CK)][ck], a principal developer in my team. The original developer to introduce me to this method. 
*  [Pavan Gutta][pg], a lead developer in my team. He helped me implement this and many such related problems. 




[tr1]: {{ site.baseurl }}{% post_url 2017-10-23-time-range-join  %}
[tr2]: {{ site.baseurl }}{% post_url 2017-12-19-time-range-join-2(time-slicing)  %}
[ck]: https://www.linkedin.com/in/chowck/
[pg]: https://www.linkedin.com/in/pavan-gutta-a94175166/
