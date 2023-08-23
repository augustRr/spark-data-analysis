import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object SparkWorldData extends App {
  val spark = SparkSession.builder()
    .master("local[8]")
    .appName("SparkWorldDataAnalysis.com")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val df = spark.read
    .option("Header", "true")
    //.option("inferSchema", "true")
    .csv("C:/Users/oguzhan/Desktop/sparkData/world-data-2023.csv")

  //  val dfc = df.withColumn("First Char",$"Country".substr(0,1))
  //    .withColumn("AvgLifeOfLang",avg($"Life expectancy")
  //        .over(Window.partitionBy("Official language")))
  //  val dfc1 = dfc.select("First Char","Official language","Life expectancy")
  //    .withColumn("AvarageLifeForCharNLang",(avg($"Life expectancy"))
  //    .over(Window.partitionBy("First Char","Official language")))
  //    .withColumn("AvarageLifeForChar", lit("100") - avg($"Life expectancy")
  //    .over(Window.partitionBy("First Char")))
  //    .withColumn("LangLivesLonger",when($"AvarageLifeForCharNLang">$"AvarageLifeForChar","Yes")
  //      .when($"AvarageLifeForCharNLang"<$"AvarageLifeForChar","No").otherwise("Equal"))
  //  dfc1.orderBy($"AvarageLifeForChar".desc).show
  val expr = $"Agricultural Land( %)".substr(lit(0), length($"Agricultural Land( %)") - 1)
  val avg_agg_df = df
    .select($"Country",$"Agricultural Land( %)",$"Unemployment rate")
    .filter($"Agricultural Land( %)".isNotNull && $"Unemployment rate".isNotNull)
    .withColumn("avg_agg", expr)//$"Agricultural Land( %)".substr(lit(0), length($"Agricultural Land( %)") - 1))

  val avgAgr = avg_agg_df
    //.map(row => row.toString().substring(1, row.toString().indexOf("%")).toDouble)
    .agg(avg($"avg_agg") as "avg_agg").head().getAs[Double](0)

  val avg_lev_df = avg_agg_df.withColumn("AgrLevel",when(expr>(avgAgr*(1.5)),4) //$"Agricultural Land( %)">
    .when(expr>avgAgr,3)
    .when(expr<avgAgr,2)
  .when(expr<(avgAgr*(0.5)),1))

  val exprUnemp = $"Unemployment rate".substr(lit("0"),length($"Unemployment rate")-1)
  val avg_unemp_df = avg_lev_df.withColumn("unemp",exprUnemp)
  val avg_unemp =avg_unemp_df.agg(avg($"unemp")).head().getDouble(0)

  val avg_agg_unemp_df = avg_unemp_df.withColumn("UnempLevel", when(exprUnemp > (avg_unemp * (1.5)), 4) //$"Agricultural Land( %)">
    .when(exprUnemp > avg_unemp, 3)
    .when(exprUnemp < avg_unemp, 2)
    .when(exprUnemp < (avg_unemp * (0.5)), 1)).select($"Country",$"UnempLevel",$"AgrLevel")

    avg_agg_unemp_df.withColumn("UnempMinusAggrLevel",$"UnempLevel"-$"AgrLevel").orderBy($"UnempMinusAggrLevel".desc).show
}
