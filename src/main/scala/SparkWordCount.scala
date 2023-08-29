import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkWordCount extends App {

  val spark = SparkSession.builder
    .master("local[8]")
    .appName("Spark Word Count")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext

  //From a text provided directly within the program
  val lines = sc.parallelize(
    Seq("Apache Spark is an open-source, " +
      "distributed computing system designed for processing " +
      "and analyzing large volumes of data in a parallel and " +
      "fault-tolerant manner. It provides a unified framework for various " +
      "data processing tasks such as batch processing, real-time streaming, " +
      "machine learning, graph processing, and more. Spark aims to make it easier " +
      "for developers to write complex data processing applications with high " +
      "performance and scalability.",
      "Scala is a programming language that was specifically designed to work " +
        "seamlessly with Spark.",
      "Spark's primary programming interface is available in multiple languages, " +
        "including Scala. Spark applications written in Scala take advantage of Scala's " +
        "expressive syntax and powerful features to create efficient and concise code for " +
        "data processing tasks."))
  val counts = lines
    .flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
  counts.foreach(println)

  println("-----------------------------------------------------------------------------")

  //From tuples & cols provided directly within the program
  var cols = Seq("Id", "Age", "Salary", "Gender")
  var data = Seq(("1", 34, 1550.80, "M"),
    ("1", 34, 1550.80, "M"),
    ("1", 34, 1550.80, "M"),
    ("1", 34, 1550.80, "M"))
  val rdd1 = spark.sparkContext.parallelize(data)
  val dfFromRdd1 = rdd1.toDF(cols: _*)
  dfFromRdd1.printSchema()
  dfFromRdd1.show()


  println("-----------------------------------------------------------------------------")

  //From a text file provided as a resource
  val textByLine = spark.read.text("src/main/resources/exampleText.txt")
  val textByWord = textByLine.select(explode(split($"value", " ")).as("word"))
  val wordCounts = textByWord.groupBy($"word")
    .agg(count("*")
      .as("count"))
    .orderBy($"count".desc)
  wordCounts.show()

  spark.stop()
}