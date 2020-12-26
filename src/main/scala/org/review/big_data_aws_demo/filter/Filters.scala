package org.review.big_data_aws_demo.filter

import org.apache.spark.sql.Row
import org.review.big_data_aws_demo.mapping.InputMapper.{away, date, home}

object Filters {

  def filterByCountry(row: Row, country: String): Boolean = {
    home(row) == country || away(row) == country
  }

  //TODO find a better way to filter by year
  def filterByYear(row: Row, year: String): Boolean = {
    date(row).contains(year)
  }

  /*
  def getGamesBetween(lowerBound: String, upperBound: String): String = {
    raw"""date BETWEEN "$lowerBound" AND "$upperBound""""
  }
  // */
}
