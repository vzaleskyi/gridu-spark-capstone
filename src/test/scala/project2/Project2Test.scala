package project2

import org.scalatest._

class Project2Test extends FunSuite {

  test("Project2.findProvocativePost") {
    val actualRes = Project2.findProvocativePost(
      "src/main/resources/user_dir.avro",
      "src/main/resources/msg_dir.avro",
      "src/main/resources/msg.avro",
      "src/main/resources/retweet.avro"
    ).rdd.map(r => (r(0), r(1), r(2), r(3), r(4), r(5))).collect.toList
    val expectedTop1Wave1 = ("FIRST",1,"Robert","Smith","text", 2)
    val expectedTop1Wave2 = ("SECOND",2,"John","Johnson","text", 5)
    assert(
      actualRes(0) === expectedTop1Wave1
        && actualRes(3) === expectedTop1Wave2
    )
  }
}