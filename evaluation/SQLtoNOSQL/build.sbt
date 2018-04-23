
lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.sqltonosql",
      scalaVersion := "2.11.8",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "SQLtoNOSQL",

    libraryDependencies += "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.13",
    libraryDependencies += "io.gatling" %% "jsonpath" % "0.6.10",
    libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0",
    libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0",
    libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.0.0",
    libraryDependencies += "org.mongodb" % "mongo-java-driver" % "3.5.0"
 )
