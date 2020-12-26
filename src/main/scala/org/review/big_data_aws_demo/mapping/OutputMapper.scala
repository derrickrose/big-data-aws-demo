package org.review.big_data_aws_demo.mapping

import org.apache.spark.sql.Row
import org.review.big_data_aws_demo.conf.Country

object OutputMapper {

  private def team(row: Row, country: Country): String = if (InputMapper.home(row).equals(country.label)) {
    InputMapper.home(row)
  } else {
    InputMapper.away(row)
  }

  private def opponent(row: Row, country: Country): String = if (!InputMapper.home(row).equals(country.label)) {
    InputMapper.home(row)
  } else {
    InputMapper.away(row)
  }

  private def teamScore(row: Row, country: Country): String = if (InputMapper.home(row).equals(country.label)) {
    InputMapper.homeScore(row)
  } else {
    InputMapper.awayScore(row)
  }

  private def opponentScore(row: Row, country: Country): String = if (!InputMapper.home(row).equals(country.label)) {
    InputMapper.homeScore(row)
  } else {
    InputMapper.awayScore(row)
  }

  def toOutputGameResult(row: Row, country: Country): Row = {
    Row(
      InputMapper.date(row),
      team(row, country),
      opponent(row, country),
      teamScore(row, country),
      opponentScore(row, country),
      InputMapper.tournament(row),
      InputMapper.city(row),
      InputMapper.country(row),
      InputMapper.neutral(row))
  }
}

//
//case class OutputGameResult(
//                             date: String,
//                             team: String,
//                             opponent: String,
//                             team_score: Int,
//                             opponent_score: Int,
//                             tournament: String,
//                             city: String,
//                             country: String,
//                             neutral: Boolean
//                           )