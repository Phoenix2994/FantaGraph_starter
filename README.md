# Java Project with JanusGraph about Lega Serie A Football

Java Project using JanusGraph 0.4.0 and Apache TinkerPop 3.4.1.

## Build Environment

* Java 8.0 Update 221
* Apache Maven 3.6.1

## Building and Running

* Datastax Distribution of Apache Cassandra 5.1.16
* Elastic Search 6.6.0
* Apache Tinkerpop 3.4.1

```
mvn clean package
mvn exec:java -Dexec.mainClass="fantagraph.graph.build.Main"
```
