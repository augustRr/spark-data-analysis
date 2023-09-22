ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"


val root = (project in file("."))
  .settings(
    name := "spark.training",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "3.4.0",
      "org.apache.hadoop" % "hadoop-common" % "3.3.2",
      "org.apache.hadoop" % "hadoop-auth" % "3.3.2",
      "org.apache.hbase" % "hbase-client" % "2.5.3",
      "org.apache.spark" %% "spark-streaming" % "3.3.2",
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.3.2",
      "org.apache.spark" %% "spark-core" % "3.3.2",
      "org.scalatra" %% "scalatra" % "2.7.0",
      "org.scalatra" %% "scalatra-json" % "2.7.0",
      "org.scalatra" %% "scalatra-scalatest" % "2.7.0" % "test",
      "ch.qos.logback" % "logback-classic" % "1.2.3" % "runtime",
      "org.eclipse.jetty" % "jetty-webapp" % "9.4.6.v20170531" % "container",
      "org.apache.spark" %% "spark-sql" % "3.3.2"
    )

  )

