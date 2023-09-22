package rest
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.json4s.DefaultFormats
import org.scalatra._
import org.scalatra.json._

class TableDataApi(spark: SparkSession) extends ScalatraServlet with JacksonJsonSupport {

  // Define implicit formats for JSON serialization/deserialization
  protected implicit lazy val jsonFormats: DefaultFormats = DefaultFormats

  // Sample data (you would load your data into a DataFrame)
  val sampleData: DataFrame = spark.createDataFrame(Seq(
    (1, "John", "Doe", 25, "Male"),
    (2, "Jane", "Smith", 30, "Female"),
    (3, "Bob", "Johnson", 22, "Male"),
    // Add more data rows here
  )).toDF("id", "first_name", "last_name", "age", "gender")

  // Define a route to get table data
  get("/table-data") {
    // Retrieve data based on the groupBy and aggregation options
    val groupedData: DataFrame = if (params.contains("groupBy")) {
      val groupByColumn = params("groupBy")
      val aggregateAverage = params.get("aggregateAverage").contains("true")
      val aggregateMax = params.get("aggregateMax").contains("true")

      val groupByExpr = col(groupByColumn)
      val aggregationExprs = Seq(
        when(aggregateAverage, avg("age")).as("average_age"),
        when(aggregateMax, max("age")).as("max_age")
      )

      sampleData.groupBy(groupByExpr).agg(aggregationExprs.head, aggregationExprs.tail: _*)
    } else {
      sampleData
    }

    // Return the grouped data as JSON
    Ok(groupedData.toJSON.collect().mkString("[", ",", "]"))
  }
}

object SparkTableDataApp extends App {
  val spark = SparkSession.builder()
    .appName("SparkTableDataApp")
    .master("local[*]") // Use your Spark cluster URL here
    .getOrCreate()

  // Create an instance of the TableDataApi and mount it on a Spark server
  val tableDataApi = new TableDataApi(spark)
  val port = 8080 // Choose a port for your API
  tableDataApi.mount("/api", tableDataApi).setPort(port)
}
