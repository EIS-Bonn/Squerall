# Evaluation
To test out the feasibility and performence of Sparkall, i.e., querying heterogeneous data residing in a Data Lake, we have generated data using Berlin Bernchmark (BSBM). We have taken 5 tables: Product, Producer, Offer, Review, and Person , and saved them in Cassandra, MySQL, MongoDB, Parquet and CSV, respectively.

We have generated three sizes of the data using the following BSBM scale factors (number of products): 500k, 1,5m, and 5m. To give a sense of the size of data, we provide how much those scale factors would generate of RDF triples: the 1,5m scale factor generates 500m triples, and the 5m scale factor generates 1,75b triples. As we took 5 tables, the previous numbers are not accurate, there are 5 more tables not considered. However, the taken tables contain most of the data.

## Queries
Original BSBM queries do not best serve Sparkall purpose, that of querying different sources. Also, as we took 5 out of 10 tables BSBM generated (under the relationa representation). Therefore, we have to alter the queries to use only the tables we chose. We replace joins with unused tables with used ones, e.g., replace vendor with producer.

we also immited three queries that have unsupported syntax: DESCRIBE (Q9), UNION (Q11), and CONSTRUCT (Q12).

Full list of queries is available in this repo along this readme.

## Docker
Docker image will be provided soon, to pull, use and evaluate Sparkall.
