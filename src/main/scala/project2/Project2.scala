/**
 * You need to read Avro files and solve the following task:
 * "A user posts a provocative message on Twitter. His subscribers do a retweet. Later, every subscriber's subscriber does retweet too."
 *
 * Find the TOP TEN USERS by a number of retweets in the FIRST AND SECOND WAVES.
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

    val wave1 = msgDF.as("msg")
      .join(
        retweetDF.as("rtw"),
        col("msg.AUTHOR_ID") === col("rtw.USER_ID")
      )
      .filter(col("msg.MESSAGE_ID") === col("rtw.MESSAGE_ID"))
      .select(
        "rtw.USER_ID",
        "rtw.SUBSCRIBER_ID",
        "rtw.MESSAGE_ID"
      )

//    wave1.show()
//    wave1
//      |USER_ID|SUBSCRIBER_ID|MESSAGE_ID|
//      +-------+-------------+----------+
//      |      1|            2|        11|
//      |      1|            3|        11|
//      |      2|            4|        12|
//      |      3|            8|        13|

    val wave1Cnt = wave1.groupBy("USER_ID", "MESSAGE_ID")
      .agg(count("SUBSCRIBER_ID").as("NUMBER_RETWEETS"))
      .orderBy(col("NUMBER_RETWEETS").desc).limit(10)
      .withColumn("WAVE", lit("FIRST"))

//    wave1Cnt.show()
//    wave1Cnt

    val wave2 = wave1.as("wave1").join(
      retweetDF.as("rtw"),
      (col("wave1.SUBSCRIBER_ID") === col("rtw.USER_ID"))
        && (col("wave1.MESSAGE_ID") === col("rtw.MESSAGE_ID")),
      "inner")
      .select(
//        col("wave1.USER_ID").as("USER_ID"),
//        col("wave1.MESSAGE_ID").as("MESSAGE_ID"),
//        col("wave1.SUBSCRIBER_ID").as("SUBSCRIBER_ID"),
        col("rtw.USER_ID"),//.as("USER_ID"),
        col("rtw.SUBSCRIBER_ID"),//.as("SUBSCRIBER_ID"),
        col("rtw.MESSAGE_ID")//.as("MESSAGE_ID")
      )

//    wave2.show()
//    wave2
//      |USER_ID|MESSAGE_ID|SUBSCRIBER_ID|USER_ID|SUBSCRIBER_ID|MESSAGE_ID|
//      +-------+----------+-------------+-------+-------------+----------+
//      |      1|        11|            2|      2|            9|        11|
//      |      1|        11|            2|      2|            8|        11|
//      |      1|        11|            2|      2|            7|        11|
//      |      1|        11|            2|      2|            6|        11|
//      |      1|        11|            2|      2|            5|        11|
//      |      1|        11|            3|      3|           17|        11|
//      |      1|        11|            3|      3|           16|        11|
//      |      1|        11|            3|      3|           15|        11|
//      |      1|        11|            3|      3|           14|        11|
//      |      1|        11|            3|      3|           13|        11|
//      |      1|        11|            3|      3|           12|        11|
//      |      1|        11|            3|      3|            7|        11|

  val wave2Cnt = wave2.groupBy("USER_ID", "MESSAGE_ID")
    .agg(count("SUBSCRIBER_ID").as("NUMBER_RETWEETS"))
    .orderBy(col("NUMBER_RETWEETS").desc).limit(10)
    .withColumn("WAVE", lit("SECOND"))

//    wave2Cnt.show()
//    wave2Cnt

    val userSub1WavesTop10Cnt = wave1Cnt.as("top")
      .join(
        userDF.as("name"),
        col("top.USER_ID") === col("name.USER_ID")
      ).join(
      msgDirDF.as("msg"),
      col("top.MESSAGE_ID") === col("msg.MESSAGE_ID")
    ).select(
      "top.WAVE",
      "top.USER_ID",
      "name.FIRST_NAME",
      "name.LAST_NAME",
      "msg.TEXT",
      "top.NUMBER_RETWEETS"
    )

    val userSub2WavesTop10Cnt = wave2Cnt.as("top")
      .join(
        userDF.as("name"),
        col("top.USER_ID") === col("name.USER_ID")
      ).join(
      msgDirDF.as("msg"),
      col("top.MESSAGE_ID") === col("msg.MESSAGE_ID")
    ).select(
      "top.WAVE",
      "top.USER_ID",
      "name.FIRST_NAME",
      "name.LAST_NAME",
      "msg.TEXT",
      "top.NUMBER_RETWEETS"
    )
    userSub1WavesTop10Cnt.union(userSub2WavesTop10Cnt)
  }

  val res =findProvocativePost("src/main/resources/user_dir.avro",
                            "src/main/resources/msg_dir.avro",
                               "src/main/resources/msg.avro",
                            "src/main/resources/retweet.avro"
                              )
//    .rdd.map(r => (r(0), r(1), r(2), r(3), r(4), r(5))).collect.toList
//    println(res(0))
//    println(res(3))
    res.show()
}


