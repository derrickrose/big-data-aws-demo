package org.review.big_data_aws_demo.mapping

import org.apache.spark.sql.Row

object InputMapper {
  def home(row: Row): String = row.getAs[String]("home_team")

  def away(row: Row): String = row.getAs[String]("away_team")

  def date(row: Row): String = row.getAs[String]("date")
}
