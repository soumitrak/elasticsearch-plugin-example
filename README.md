# elasticsearch-plugin-example

Elasticsearch plugin example in Scala

I am going to write a set of streaming aggregations in elasticsearch, so this is in preparation of that to understand how plugins work in elasticsearch.

How to build and install plugin
1. Build the project and create ZIP archive
mvn package

2. Delete existing plugin from elasticsearch
elasticsearch/bin/plugin --remove elasticsearch-plugin-example

3. Install plugin in elasticsearch
elasticsearch/bin/plugin --url file:///elasticsearch-plugin-example/target/releases/elasticsearch-plugin-example-1.0-SNAPSHOT.zip --install elasticsearch-plugin-example

There are two metric aggregation functions in this package:

1. countdistinctn - Calculate number of distinct values in a numeric field.

2. countdistinct - Calculate number of distinct values in a numeric or string field.

Both countdistinctn and countdistinct are implemented using Set with space complexity of O(n). This will may not scale for big data domain if the number of distinct elements can be very large. That is where my journey into Probabilistic data structures starts [https://en.wikipedia.org/wiki/Category:Probabilistic_data_structures] .
