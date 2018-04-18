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

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0"

libraryDependencies += "org.apache.jena" % "jena-core" % "3.1.1"
libraryDependencies += "org.apache.jena" % "jena-arq" % "3.1.1"

libraryDependencies += "com.google.guava" % "guava" % "22.0"

libraryDependencies += "com.typesafe.play" % "play_2.11" % "2.6.2"

// NOTE: the suffix should match with scala version used
libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.0.0"

libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.1.0"

// https://mvnrepository.com/artifact/mysql/mysql-connector-java
libraryDependencies += "mysql" % "mysql-connector-java" % "6.0.6"

dependencyOverrides ++= Set("com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5")

//libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.1.0"

// https://mvnrepository.com/artifact/org.mongodb/mongo-java-driver
libraryDependencies += "org.mongodb" % "mongo-java-driver" % "3.5.0"
