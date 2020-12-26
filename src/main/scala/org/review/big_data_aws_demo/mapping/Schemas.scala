package org.review.big_data_aws_demo.conf

import org.apache.spark.sql.types._

object Schemas {
  private val homeTeam = notNullableString("home_team")
  private val awayTeam = notNullableString("away_team")
  private val homeScore = notNullableString("home_score")
  private val awayScore = notNullableString("away_score")

  private val date = notNullableString("date")
  private val tournament = nullableString("tournament")
  private val city = nullableString("city")
  private val country = nullableString("country")
  private val neutral = nullableString("neutral")
  val InputGameResult = StructType(Seq(date, homeTeam, awayTeam, homeScore, awayScore, tournament, city, country, neutral))

  private val team = notNullableString("team")
  private val opponent = notNullableString("opponent")
  private val asHome = notNullableBoolean("as_home")
  private val teamScore = notNullableString("team_score")
  private val opponentScore = notNullableString("opponent_score")
  val OutputGameResult = StructType(Seq(date, team, opponent, asHome, teamScore, opponentScore, tournament, city, country, neutral))

  private def nullableString(fieldName: String) = stringField(true, fieldName)

  private def notNullableString(fieldName: String) = stringField(false, fieldName)

  private def stringField(nullable: Boolean, fieldName: String) = StructField(fieldName, StringType, nullable)

  private def notNullableBoolean(fieldName: String) = booleanField(false, fieldName)

  private def booleanField(nullable: Boolean, fieldName: String) = StructField(fieldName, BooleanType, nullable)

}
