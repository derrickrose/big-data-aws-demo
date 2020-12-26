package org.review.big_data_aws_demo

//import org.review.big_data_aws_demo.conf.Schema

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class SoccerGamesFilteringTest extends FlatSpec with SparkTestContext with Matchers with BeforeAndAfterAll {

  "Games dataFrame" should "not be empty" in {
    //val footGamesDF = TestSparkSession.read.schema(Schema.GameResult)
    val footGamesDF = TestSparkSession.read
      .option("mode", "permissive")
      .option("header", true)
      .csv("files/foot.csv")

    //footGamesDF should not be empty
    assert(footGamesDF.count() > 0, "Games dataFrame should not be empty")
  }

  //TODO integration test
}
