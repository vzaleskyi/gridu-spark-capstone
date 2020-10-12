/**
 * You need to read Avro files and solve the following task:
 * "A user posts a provocative message on Twitter. His subscribers do a retweet. Later, every subscriber's subscriber does retweet too."
 * Find the top ten users by a number of retweets in the first and second waves.
 */
package project2

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window

object Project2 extends App{

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  def findProvocativePost(userDataPath: String, userMsgDataPath: String, msgDataPath: String, retweetDataPath: String): DataFrame = {

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("Project2")
        .master("local")
        .getOrCreate()

    val userDF = spark.read.format("avro").load(userDataPath)
    val msgDirDF = spark.read.format("avro").load(userMsgDataPath)
    val msgDF = spark.read.format("avro").load(msgDataPath)
    val retweetDF = spark.read.format("avro").load(retweetDataPath)

    val windowAgg  = Window.partitionBy("USER_ID")
    val aggDf = retweetDF.withColumn("sub_cnt",count("SUBSCRIBER_ID").over(windowAgg)).select("*")

    val userSubWavesCnt = aggDf.as("wave1").join(
      aggDf.as("wave2"),
      (col("wave1.SUBSCRIBER_ID") === col("wave2.USER_ID"))
        && (col("wave1.MESSAGE_ID") === col("wave2.MESSAGE_ID")),"inner")
      .select(
        col("wave1.USER_ID").as("USER_ID"),
        col("wave1.MESSAGE_ID").as("MESSAGE_ID"),
        col("wave1.sub_cnt").as("1st_wave_cnt"),
        col("wave2.sub_cnt").as("2d_wave_cnt"),
      )
      .groupBy(col("USER_ID"), col("MESSAGE_ID"), col("1st_wave_cnt"))
      .agg(max(col("2d_wave_cnt")).as("2d_wave_cnt_max"))
      .withColumn("NUMBER_RETWEETS", col("1st_wave_cnt")+col("2d_wave_cnt_max"))

//    userSubWavesCnt.show()
//    userSubWavesCnt

    // select all
//      +-------+-------------+----------+-------+-------+-------------+----------+-------+
//      |USER_ID|SUBSCRIBER_ID|MESSAGE_ID|sub_cnt|USER_ID|SUBSCRIBER_ID|MESSAGE_ID|sub_cnt|
//      +-------+-------------+----------+-------+-------+-------------+----------+-------+
//      |      1|            2|        11|      2|      2|            5|        11|      6|
//      |      1|            2|        11|      2|      2|            7|        11|      6|
//      |      1|            2|        11|      2|      2|            9|        11|      6|
//      |      1|            2|        11|      2|      2|            6|        11|      6|
//      |      1|            2|        11|      2|      2|            8|        11|      6|
//      |      1|            3|        11|      2|      3|            7|        11|      2|
//      |      2|            5|        11|      6|      5|           33|        11|      1|
//      |      3|            7|        11|      2|      7|           14|        11|      1|
//      |      2|            7|        11|      6|      7|           14|        11|      1|
//      +-------+-------------+----------+-------+-------+-------------+----------+-------+

    val userSubWavesTop10Cnt = userSubWavesCnt
      .select(
        col("USER_ID"),
        col("MESSAGE_ID"),
        col("NUMBER_RETWEETS")
      )
      .distinct()
      .orderBy(col("NUMBER_RETWEETS").desc).limit(10)

//    userSubWavesTop10Cnt.show()
//    userSubWavesTop10Cnt

    userSubWavesTop10Cnt.as("top")
      .join(
        userDF.as("name"),
        col("top.USER_ID") === col("name.USER_ID")
      ).join(
      msgDirDF.as("msg"),
      col("top.MESSAGE_ID") === col("msg.MESSAGE_ID")
    ).select(
      "top.USER_ID",
      "name.FIRST_NAME",
      "name.LAST_NAME",
      "msg.TEXT",
      "top.NUMBER_RETWEETS"
    )
  }

  val res =findProvocativePost("src/main/resources/user_dir.avro",
                            "src/main/resources/msg_dir.avro",
                               "src/main/resources/msg.avro",
                            "src/main/resources/retweet.avro"
                              )
//  res.show()
  //  val res = findProvocativePost(userDirReadDf, msgDirReadDf, msgReadDf, retweetReadDf)
  //               .rdd.map(r => (r(0), r(1), r(2), r(3), r(4))).collect.toList
  //  println(res(0))
}


