import SparkWorldData.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SparkWordCount extends App {

  val spark = SparkSession.builder
    .master("local[8]")
    .appName("Spark Word Count")
  .getOrCreate()
  import spark.implicits._
  case class Person(name:String,surname:String)
  val df= Seq(Person("ally","menra")).toDF
  df.show
  val lines = spark.sparkContext.parallelize(
    Seq("Spark Intellij Idea Scala test one",
      "Spark Intellij Idea Scala test two",
      "Spark Intellij Idea Scala test three"))
val col1 = col("str")
  val counts = lines
    .flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)

  counts.foreach(println)
  println("-----------------------------------------------------------")
  var cols = Seq("Id", "Age", "Salary", "Gender")
  var data = Seq(("1", 34, 1550.80, "M"),
    ("1", 34, 1550.80, "M"),
    ("1", 34, 1550.80, "M"),
    ("1", 34, 1550.80, "M"))
  val rdd1 = spark.sparkContext.parallelize(data)
  val dfFromRdd1 = rdd1.toDF("Id", "Age", "Salary", "Gender")
  dfFromRdd1.printSchema()
  dfFromRdd1.show()
}