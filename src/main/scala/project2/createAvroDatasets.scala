package project2

import org.apache.spark.sql.SparkSession

object createAvroDatasets extends App {

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("CreateDatasets")
      .master("local")
      .getOrCreate()

  val userDirData = Seq(
    (1,"Robert","Smith"),
    (2,"John","Johnson"),
    (3,"Alex","Jones"),
    (4,"John1","Johnson"),
    (5,"John2","Johnson"),
    (6,"John3","Johnson"),
    (7,"John4","Johnson"),
    (8,"John5","Johnson"),
    (9,"John6","Johnson"),
    (10,"John7","Johnson"),
    (11,"John8","Johnson")
  )

  val msgDirData = Seq(
    (11,"text"),
    (12,"text"),
    (13,"text")
  )
  val msgData = Seq(
    (1,11),
    (2,12),
    (3,13)
  )
  val retweetData = Seq(
    (1,2,11),
    (1,3,11),
    (2,5,11),
    (2,6,11),
    (2,7,11),
    (2,8,11),
    (2,9,11),
    (3,7,11),
    (7,14,11),
    (5,33,11),
    (2,4,12),
    (3,8,13)
  )

  import spark.sqlContext.implicits._

  val userDirShema = Seq("USER_ID", "FIRST_NAME", "LAST_NAME")
  val msgDirShema = Seq("MESSAGE_ID", "TEXT")
  val msgShema = Seq("USER_ID", "MESSAGE_ID")
  val retweetShema = Seq("USER_ID", "SUBSCRIBER_ID", "MESSAGE_ID")

  val userDirDf = userDirData.toDF(userDirShema:_*)
  val msgDirDf = msgDirData.toDF(msgDirShema:_*)
  val msgDf = msgData.toDF(msgShema:_*)
  val retweetDf = retweetData.toDF(retweetShema:_*)

  userDirDf.write.format("avro").save("src/main/resources/user_dir.avro")
  msgDirDf.write.format("avro").save("src/main/resources/msg_dir.avro")
  msgDf.write.format("avro").save("src/main/resources/msg.avro")
  retweetDf.write.format("avro").save("src/main/resources/retweet.avro")
}
