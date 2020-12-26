package org.review.big_data_aws_demo.mapping

import org.apache.spark.sql.Row

object InputMapper {
  def date(row: Row): String = row.getAs[String]("date")

  def home(row: Row): String = row.getAs[String]("home_team")

  def away(row: Row): String = row.getAs[String]("away_team")

  def homeScore(row: Row): String = row.getAs[String]("home_score")

  def awayScore(row: Row): String = row.getAs[String]("away_score")

  def tournament(row: Row): String = row.getAs[String]("tournament")

  def city(row: Row): String = row.getAs[String]("city")

  def country(row: Row): String = row.getAs[String]("country")

  def neutral(row: Row): String = row.getAs[String]("neutral")
}


