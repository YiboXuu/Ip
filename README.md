## Ip

Name: Yibo Xu 
Id: 21071703
This project implements a streaming data processing solution for continuous TPC-H Q3 query evaluation using Apache Flink. The implementation transforms the traditional SQL query into a dynamic streaming pipeline that processes order and line item data in real-time, filtering by market segment and date criteria before computing revenue aggregations. The architecture leverages Flink's stateful operators to maintain join relationships between customer, order, and line item tables, while efficiently updating the top-10 revenue results as new data arrives. Performance optimizations include specialized processors for each transformation step and appropriate parallelism configuration to handle high-throughput scenarios. This work demonstrates practical application of streaming query processing techniques in a real-world benchmark context.
