ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"


val root = (project in file("."))
  .settings(
    name := "spark.training",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "3.4.0",
      "org.apache.hbase" % "hbase-client" % "2.5.3",
      "org.apache.spark" %% "spark-streaming" % "3.3.2",
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.3.2",
      "org.apache.spark" %% "spark-core" % "3.3.2",
      "org.apache.spark" %% "spark-sql" % "3.3.2"
    )
  )
