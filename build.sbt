ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

autoScalaLibrary := false

lazy val root = (project in file("."))
  .settings(
    name := "SparkCassandraCommunication"
  )

val sparkVersion = "3.1.2"
val sparkCassandraConnectorVersion = "3.1.0"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

val sparkCassandraConnectorDependencies = Seq(
  "com.datastax.spark" %% "spark-cassandra-connector" % sparkCassandraConnectorVersion,
  "joda-time" % "joda-time" % "2.12.1"
)

libraryDependencies ++= sparkDependencies
libraryDependencies ++= sparkCassandraConnectorDependencies
