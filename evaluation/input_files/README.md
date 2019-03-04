[![Gitter](https://img.shields.io/gitter/room/DAVFoundation/DAV-Contributors.svg?style=flat-square)](https://gitter.im/sparkall)

# Evaluation
To test out the feasibility and performence of Sparkall, i.e., querying heterogeneous data residing in a Data Lake, we have generated data using Berlin Bernchmark (BSBM). We have taken 5 tables: Product, Producer, Offer, Review, and Person , and saved them in Cassandra, MySQL, MongoDB, Parquet and CSV, respectively.

We have generated three sizes of the data using the following BSBM scale factors (number of products): 500k, 1,5m, and 5m. To give a sense of the size of data, we provide how much those scale factors would generate of RDF triples: the 1,5m scale factor generates 500m triples, and the 5m scale factor generates 1,75b triples. As we took 5 tables, the previous numbers are not accurate, there are 5 more tables not considered. However, the taken tables contain most of the data.

## Queries
Original BSBM queries touch only 1~3 tables at once at most. That does not serve best Sparkall purpose, that of querying several data sources in one query. We therefore had to alter the queries so to use more tables. Further, some of BSBM queries use tables outside of the set of tables we chose, like Vendor, se we repaced unused tables with used ones, e.g., Vendor with Producer. We also ommited three queries that have unsupported syntax: DESCRIBE (Q9), UNION (Q11), and CONSTRUCT (Q12).

Full list of queries is available in this repo along this README.

SPARQL queries are expressed using the Ontology terms the data was previously mapped to. SPARQL query should conform to the currently supported SPARQL fragment:

SPARQL fragment supported is:
```SPARQL
Query       := Prefix* SELECT Distinguish WHERE{ Clauses } Modifiers?
Prefix      := PREFIX "string:" IRI
Distinguish := DISTINCT? (“*”|(Var|Aggregate)+)
Aggregate   := (AggOpe(Var) ASVar)
AggOpe      := SUM|MIN|MAX|AVG|COUNT
Clauses     := TP* Filter?
Filter      := FILTER (Var FiltOpe Litteral)
             | FILTER regex(Var, "%string%")
FiltOpe     :==|!=|<|<=|>|>=
TP          := VarIRIVar .|Varrdf:type IRI.
Var         := "?string"
Modifiers   := (LIMITk)? (ORDER BY(ASC|DESC)? Var)? (GROUP BYVar+)?
```

## Docker
We provide a [Dockerfile](https://github.com/EIS-Bonn/sparkall/blob/master/Dockerfile) to reproduce the conducted experiments. It downloads Spark and installs three databases: Cassandra, MongoDB and MySQL. It downloads the BSBM data generator and generates a small dataset. It then runs a tailored (for BSBM) Spark-based data loader, which loads BSBM data to the aformentioned databases and files. Finally it gets the queries from this repository and runs over them Sparkall.

- Build the image as usual. Change directory to where the Dockerfile is and run: `docker build -t sparkall .`.
- Run the image as usual. Run: `docker run -it sparkall`*. You will get a welcome screen explaining to you what you see and how to proceed.

You will be logged in to a functioning Ubuntu system with root user. The system has Cassandra, MongoDB and MySQL preloaded with the experiments data. For example you can see your data inside those databases, just log to their respective CLIs:
```
cqlsh # Cassandra
mongo # MongoDB
mysql -u root -p # MYSQL, type 'root' as password
```
However, you could use this system to load other data and run other queries. Just remember to provide `config` and `mappings` files, go back to repository README for explanation.

**Note:** if you get `cannot create /proc/sys/vm/drop_caches: Read-only file system` error running `load-data.sh` script, start docker run command with the `--previleged` option (see [1](https://unix.stackexchange.com/questions/209244/which-linux-capability-do-i-need-in-order-to-write-to-proc-sys-vm-drop-caches/209412#209412)).
If you get `cannot open shared object file: Permission denied` error running `run-sparkall.sh`, restart Docker daemon with `-s="devicemapper"` option (see [2](https://stackoverflow.com/questions/22473830/docker-and-mysql-libz-so-1-cannot-open-shared-object-file-permission-denied) [3](https://github.com/moby/moby/issues/7512)). This issue does not affect the query execution process, the program finishes till the end and results are shown. It rather originates from the process of clearing the cache. If you find a solution/workaround, please contribute it to the project.
