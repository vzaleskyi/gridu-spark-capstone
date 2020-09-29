/**
Assume we have a system to store information about warehouses and amounts of goods.

We have the following data with formats:
List of warehouse positions. Each record for a particular position is unique. Format: { positionId: Long, warehouse: String, product: String, eventTime: Timestamp }
List of amounts. Records for a particular position can be repeated. The latest record means the current amount. Format: { positionId: Long, amount: BigDecimal, eventTime: Timestamp }.

Using Apache Spark, implement the following methods using DataFrame/Dataset API:

Load required data from files, e.g. csv of json.
Find the current amount for each position, warehouse, product.
Find max, min, avg amounts for each warehouse and product.
 */
package project1

import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, Row}

object Project1 extends App{

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Project1")
      .master("local")
      .getOrCreate()

  import spark.implicits._

  // Load required data from files
  val positionDF = spark.read
                        .option("inferSchema", "true")
                        .option("header", "true")
                        .csv("src/main/resources/positions.csv")
  val amountsDF = spark.read
                        .option("header", "true")
                        .option("inferSchema", "true")
                        .csv("src/main/resources/amounts.csv")

  val inter2: DataFrame = amountsDF.groupBy("positionId").max("eventTime").withColumnRenamed("max(eventTime)", "eventTime").select($"positionId", $"eventTime")
//  inter2.show()
//  +----------+----------+
//  |positionId| eventTime|
//  +----------+----------+
//  |         1|1528463098|
//  |         3|1528463111|
//  |         4|1528463112|
//  |         2|1528463100|
//  +----------+----------+
  val currentAmount = inter2.as("i").join(
      amountsDF.as("a"),
      ($"i.positionId" <=>  $"a.positionId")
        && ($"i.eventTime" <=>  $"a.eventTime"), "inner")
.select($"a.positionId", $"a.amount")
//  +----------+------+
//  |positionId|amount|
//  +----------+------+
//  |         1|  10.0|
//  |         2|   5.0|
//  |         3|   5.5|
//  |         4| 99.57|
//  +----------+------+

// current amount for each position, warehouse, product
  val currentAmountForeach = currentAmount.as("c").join(
    positionDF.as("p"),
    $"c.positionId" === $"p.positionId",
    "inner"
).select($"c.positionId", $"p.warehouse", $"p.product", $"c.amount")
//    +----------+---------+-------+------+
//    |positionId|warehouse|product|amount|
//    +----------+---------+-------+------+
//    |         1|      W-1|    P-1|  10.0|
//    |         2|      W-1|    P-2|   5.0|
//    |         3|      W-2|    P-3|   5.5|
//    |         4|      W-2|    P-4| 99.57|
//    +----------+---------+-------+------+

//  max, min, avg amounts for each warehouse and product
//  W-1, P-1, <max?>, <min?>, <avg?>
  val inter = amountsDF.as("a").join(
    positionDF.as("p"),
    $"a.positionId" === $"p.positionId",
    "inner"
  ).select($"a.positionId", $"p.warehouse", $"p.product", $"a.amount")
//    +----------+---------+-------+------+
//    |positionId|warehouse|product|amount|
//    +----------+---------+-------+------+
//    |         1|      W-1|    P-1|  10.0|
//    |         1|      W-1|    P-1|  10.2|
//    |         2|      W-1|    P-2|   5.0|
//    |         3|      W-2|    P-3|   4.9|
//    |         3|      W-2|    P-3|   5.5|
//    |         3|      W-2|    P-3|   5.0|
//    |         4|      W-2|    P-4| 99.99|
//    |         4|      W-2|    P-4| 99.57|
//    +----------+---------+-------+------+
  val maxAmount = inter.groupBy($"warehouse", $"product").agg(max($"amount"))//.show()
  val minAmount = inter.groupBy($"warehouse", $"product").agg(min($"amount"))//.show()
  val avgAmount = inter.groupBy($"warehouse", $"product").agg(avg($"amount"))//.show()
}
