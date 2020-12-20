package org.review.big_data_aws_demo.bootcamp_itab_academy

object FilterFootGamesByCountry {


  def main(args: Array[String]): Unit = {

    val configuration: Configuration = fromSparkSubmit(args)

    import org.apache.spark.sql.SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("Spark app to filter foot-ball games by country")
      .getOrCreate()

    val footGamesDF = sparkSession.read.option("header", true).csv(buildCollectionPath(configuration.inputPath))
    import sparkSession.implicits._
    val filteredDF = footGamesDF.filter($"home_team" === s"${configuration.country}" || $"away_team" === s"${configuration.country}")
    filteredDF.write.json(configuration.outputPath)
  }

  def buildCollectionPath(inputPath: String): String = {
    s"${inputPath}/*"
  }

  def fromSparkSubmit(args: Array[String]): Configuration = {
    Configuration(args(0), args(1), args(2))
  }

  case class Configuration(inputPath: String, country: String, outputPath: String) {

  }

}
