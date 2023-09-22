
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Country Analysis")
    .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.expressions._

  val df = readCountryData(spark)
  val dfCon = spark.read.option("header", "true").csv("C:\\Users\\oguzhan\\Desktop\\sparkData\\Countries-Continents.csv")
  val dfJoin = df.join(dfCon, Seq("Country"), "right") //df("Country") === dfCon("Country"),"Right")
  val dfNeigh = spark.read.option("header","true").csv("C:/Users/oguzhan/Desktop/sparkData/neighbors.csv")
  val df3 = dfJoin.join(dfNeigh,dfJoin("Country") === dfNeigh("country_name"),"right")
  val df4 = df3.join(dfJoin.select($"CPI" as "Neighbor CPI",$"Country" as "neighbor") as "dfShort", $"dfShort.Country" === df3("country_border_name"),"inner").drop("neighbor")

  val removeDollarAndConvert: String => Double = (amount: String) => amount.substring(1).toDouble
  val removeDollarAndConvertUDF = udf(removeDollarAndConvert)


//  dfJoin.where($"Minimum wage".isNotNull)
//    .groupBy($"Continent")
//    .agg(avg(removeDollarAndConvertUDF($"Minimum wage")) as "Avarage MinWage By Continent").show


  val replaceComma = (str: String) => str.replace(",","").toDouble
  val replaceUdf = udf(replaceComma)

  val globalArmedForceAvg = dfJoin.select(avg(regexp_replace($"Armed Forces size",",","").cast(DoubleType))).first().getDouble(0)

//  df3.withColumn("CPIOfNeigh",)
//    withColumn("diffOfCPIBtwNegs",

//  df3.where($"country_name".startsWith("T")).groupBy("country_name").count().show

//  dfJoin.select($"Fertility Rate",$"Continent", avg(coalesce($"Fertility Rate", lit(0))).over(Window.partitionBy($"Continent")) as "replacdBy0")
//    .where($"Fertility Rate".isNotNull)
//    .withColumn("replacedNull",avg($"Fertility Rate").over(Window.partitionBy("Continent")))
//    .drop($"Fertility Rate")
//    .distinct().show

//  dfJoin.select($"Country",$"Fertility Rate")
//  .where($"Fertility Rate".isNotNull)
//  .orderBy($"Fertility Rate".desc_nulls_last)
//  .limit(5)
//  .show(false)

//  dfJoin.where($"Armed Forces size".isNotNull)
//    .withColumn("Military/Global", regexp_replace($"Armed Forces size",",","").cast(DoubleType)/globalArmedForceAvg)
//    .show

//  dfJoin.select($"Continent",avg($"Birth Rate").over(Window.partitionBy($"Continent")) as "Avg Birth Rate by Continent")
//    .groupBy("Continent")
//    .agg(avg("Avg Birth Rate by Continent") as "Final Average")
//    .show

//  dfJoin.select("Country","CPI","Fertility Rate")
//    .withColumn("RankByCPI",rank().over(Window.orderBy($"CPI".desc_nulls_last)))
//    .orderBy($"Fertility Rate".desc_nulls_last)
//    .limit(5)
//    .show

  //    df.withColumn("AvgInfMortByCurr",avg($"Infant mortality")
  //      .over(Window.partitionBy("Currency-Code")))
  //      .withColumn("countByCurr",count($"Country").over(Window.partitionBy("Currency-Code")))
  //      .where($"countByCurr" > 1)
  //    .withColumn("InfMortOverAvg", when($"Infant Mortality" >= $"AvgInfMortByCurr", "yes")
  //      .otherwise("no"))
  //      .select("Country","InfMortOverAvg","Gross primary education enrollment (%)")
  //      .orderBy($"Gross primary education enrollment (%)".desc_nulls_first).show

  //    df.withColumn("CallGroup",$"Calling Code"-$"Calling Code"%5)
  //      .withColumn("BirthGroup",$"Fertility Rate".cast("Int"))
  //      .where($"Gasoline Price".isNotNull)
  //    .groupBy($"CallGroup",$"BirthGroup")
  //      .agg(avg(removeDollarAndConvertUDF($"Gasoline Price")) as "avg Gas Price").show()


  def readCountryData(spark: SparkSession) = {
    spark.read
      .option("header", "true")
      .csv("C:/Users/oguzhan/Desktop/sparkData/world-data-2023.csv")
  }

}
