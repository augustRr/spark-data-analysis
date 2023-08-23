import SparkWorldData.spark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object SparkWorldDataRefined extends App {


  // Create a SparkSession (only needed if you're not already in a Spark session)
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("DataFrameProcessing")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  // Read the CSV file
  val df1 = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("C:/Users/oguzhan/Desktop/sparkData/world-data-2023.csv")
    .select("Country", "Agricultural Land( %)", "Unemployment rate")
    .filter($"Agricultural Land( %)".isNotNull and $"Agricultural Land( %)" =!= "")
    .filter($"Unemployment rate".isNotNull and $"Unemployment rate" =!= "")

  // Define an expression to remove the percentage symbol and convert to Double
  val removePercentage = udf((value: String) => {
        value.stripSuffix("%").toDouble
  })

  val avg_agri = df1
    .select(avg(removePercentage($"Agricultural Land( %)"))).head().getDouble(0)


  // Calculate average values for Agricultural Land and Unemployment rate
  //val avgAgr = df.select(avg(removePercentage($"Agricultural Land( %)")))
  val avgUnemp = df1.select(avg(removePercentage($"Unemployment rate"))).head().getDouble(0)

  // Define categorization logic for levels
  def categorizeLevel(value: Column, avgValue: Column): Column = {
    lit(
      when(value > avgValue * 1.5,4)
      .when(value >= avgValue,3)
      .when(value < avgValue,2)
      .when(value <= avgValue * 0.5,1)
        .otherwise(0)
    )
  }

  // Categorize Agricultural Land and Unemployment rate levels
  val categorizedDF = df1
    .withColumn("AgrLevel", lit(categorizeLevel(removePercentage($"Agricultural Land( %)"), lit(avg_agri))))
    .withColumn("UnempLevel", lit(categorizeLevel(removePercentage($"Unemployment rate"), lit(avgUnemp))))

  // Calculate the difference between Unemployment Level and Agricultural Land Level
  val resultDF = categorizedDF
    .withColumn("UnempMinusAggrLevel", $"UnempLevel" - $"AgrLevel")
    .orderBy($"UnempMinusAggrLevel".desc)
    .select($"Country", $"UnempLevel", $"AgrLevel", $"UnempMinusAggrLevel")


  // Show the result
  resultDF.show()

  // Stop the SparkSession
  spark.stop()

}
