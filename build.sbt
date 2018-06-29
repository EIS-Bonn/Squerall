test in assembly := {}

assemblyJarName in assembly := "sparkall_01.jar"

mainClass in assembly := Option("org.sparkall.Main")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", "DUMMY.SF") => MergeStrategy.discard
  case _ => MergeStrategy.first
}

name := "Sparkall"

version := "0.1"

scalaVersion := "2.11.8"

// Utilities
libraryDependencies += "com.google.guava" % "guava" % "22.0"

// Spark dependencies
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0"

// Language parsers
libraryDependencies += "org.apache.jena" % "jena-core" % "3.1.1"
libraryDependencies += "org.apache.jena" % "jena-arq" % "3.1.1"

libraryDependencies += "com.typesafe.play" % "play_2.11" % "2.6.2"

libraryDependencies += "io.gatling" %% "jsonpath" % "0.6.10"

// Connector dependencies
// NOTE: the suffix should match with scala version used
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.8"

libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.2.2"

libraryDependencies += "com.couchbase.client" %% "spark-connector" % "2.1.0" // 2.3.0 didn't work

libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "6.2.4"

libraryDependencies += "mysql" % "mysql-connector-java" % "6.0.6"

// https://mvnrepository.com/artifact/com.datastax.cassandra/cassandra-driver-core
//libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.3.2" // 3.4.0 & 3.5.0 conflicts with mongodb

dependencyOverrides ++= Set("com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5")

// https://mvnrepository.com/artifact/org.mongodb.scala/mongo-scala-driver
//libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.2.1"

// https://mvnrepository.com/artifact/com.h2database/h2
libraryDependencies += "com.h2database" % "h2" % "1.4.197"

// https://mvnrepository.com/artifact/com.facebook.presto/presto-jdbc
libraryDependencies += "com.facebook.presto" % "presto-jdbc" % "0.204"


