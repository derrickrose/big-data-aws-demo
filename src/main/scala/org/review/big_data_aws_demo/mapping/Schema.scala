//package org.review.big_data_aws_demo.conf
//
//import org.apache.spark.sql.types._
//
//object Schema {
//
//  val date = notNullableString("date")
//  val homeTeam = notNullableString("home_team")
//  val awayTeam = notNullableString("away_team")
//  val homeScore = notNullableInteger("home_score")
//  val awayScore = notNullableInteger("away_score")
//  val tournament = nullableString("tournament")
//  val city = nullableString("city")
//  val country = nullableString("country")
//  val neutral = nullableBoolean("neutral")
//
//  val GameResult = StructType(Seq(date, homeTeam, awayTeam, homeScore, awayScore, tournament, city, country, neutral))
//
//  private def nullableString(fieldName: String) = stringField(true, fieldName)
//
//  private def notNullableString(fieldName: String) = stringField(false, fieldName)
//
//  private def stringField(nullable: Boolean, fieldName: String) = StructField(fieldName, StringType, nullable)
//
//  private def nullableDouble(fieldName: String) = doubleField(true, fieldName)
//
//  private def doubleField(nullable: Boolean, fieldName: String) = StructField(fieldName, DoubleType, nullable)
//
//  private def nullableBoolean(fieldName: String) = booleanField(true, fieldName)
//
//  private def notNullableBoolean(fieldName: String) = booleanField(false, fieldName)
//
//  private def booleanField(nullable: Boolean, fieldName: String) = StructField(fieldName, BooleanType, nullable)
//
//  private def notNullableInteger(fieldName: String) = integerField(false, fieldName)
//
//  private def nullableInteger(fieldName: String) = integerField(true, fieldName)
//
//  private def integerField(nullable: Boolean, fieldName: String) = StructField(fieldName, IntegerType, nullable)
//
//  private def notNullableLong(fieldName: String) = longField(false, fieldName)
//
//  private def nullableLong(fieldName: String) = longField(true, fieldName)
//
//  private def longField(nullable: Boolean, fieldName: String) = StructField(fieldName, LongType, nullable)
//
//}