package org.review.big_data_aws_demo.filter

import org.apache.spark.sql.DataFrame
import org.review.big_data_aws_demo.SparkTestContext
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class FiltersTest extends FlatSpec with SparkTestContext with Matchers with BeforeAndAfterAll {

  import TestSparkSession.implicits._

  var games: DataFrame = _

  override def beforeAll(): Unit = {
    val franceAsHome = FilterTestUtils.resultWithCountryAsHome(country = "France", date = "2020-11-10")
    val franceAsVisitor = FilterTestUtils.resultWithCountryAsVisitor(country = "France")
    val spainAsHome = FilterTestUtils.resultWithCountryAsHome(country = "Spain")
    val spainAsVisitor = FilterTestUtils.resultWithCountryAsVisitor(country = "Spain")
    val unitedStatesAsHome = FilterTestUtils.resultWithCountryAsHome(country = "United Sates")
    games = Seq(franceAsHome, franceAsVisitor, spainAsHome, spainAsVisitor, unitedStatesAsHome).toDF()
  }

  """behavior of filter by country"""

  "it" should "keep France games" in {
    games.filter(Filters.filterByCountry(_, "France")).count() shouldBe 2
  }

  "it" should "be empty" in {
    games.filter(Filters.filterByCountry(_, "Morocco")).count() shouldBe 0
  }

  """behavior of filter by date"""
  //*
  "it" should "keep 4 games" in {
    games.filter(Filters.filterByYear(_, "2019")).count() shouldBe 4
  }
  // */

  "it" should "keep 1 game" in {
    games.filter(Filters.filterByYear(_, "2020")).count() shouldBe 1
  }


}

object FilterTestUtils {

  case class InputGameResult(date: String = "2019-01-25", home_team: String = "", away_team: String = "",
                             home_score: String = "", away_score: String = "", tournament: String = "",
                             city: String = "", country: String = "", neutral: String = "") {

  }

  def resultWithCountryAsHome(country: String, date: String = "2019-01-25"): InputGameResult = {
    InputGameResult(home_team = country, date = date)
  }

  def resultWithCountryAsVisitor(country: String, date: String = "2019-01-25"): InputGameResult = {
    InputGameResult(away_team = country, date = date)
  }

}