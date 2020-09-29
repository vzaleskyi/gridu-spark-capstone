package project2

import org.scalatest._

class Project2Test extends FunSuite {

  test("Project2.findProvocativePost") {
    val actualRes = Project2.findProvocativePost(
      "src/main/resources/user_dir.avro",
      "src/main/resources/msg_dir.avro",
      "src/main/resources/msg.avro",
      "src/main/resources/retweet.avro"
    ).rdd.map(r => (r(0), r(1), r(2), r(3), r(4))).collect.toList
    val expectedRes = (1,"Robert","Smith","text",4)
    assert(
      actualRes(0) === expectedRes
    )
  }
}