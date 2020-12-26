package org.review.big_data_aws_demo


import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.review.big_data_aws_demo.conf.{Configuration, Country, ExecutionConfiguration, Schemas}
import org.review.big_data_aws_demo.filter.Filters
import org.review.big_data_aws_demo.mapping.OutputMapper

object SoccerGamesFiltering extends App {

  override def main(args: Array[String]): Unit = {

    val configuration: ExecutionConfiguration = fromSparkSubmit(args)

    import org.apache.spark.sql.SparkSession
    val sparkSession = SparkSession.builder()
      .appName("soccer games filtering by country")
      .getOrCreate()

    val footGamesDF = sparkSession.read
      .option("mode", "permissive")
      .option("header", true)
      .csv(buildCollectionInputPath(configuration.inputPath))

    val filteredDF = footGamesDF
      .filter(Filters.filterByYear(_, configuration.year))
      .filter(Filters.filterByCountry(_, configuration.country.label))

    val encoder = RowEncoder(Schemas.OutputGameResult)
    val outputDF = filteredDF.map(OutputMapper.toOutputGameResult(_, configuration.country))(encoder)
    outputDF.write.mode(SaveMode.Overwrite).json(buildCollectionOutputPath(configuration))
  }

  def buildCollectionInputPath(inputPath: String): String = {
    s"${inputPath}/*"
  }

  def buildCollectionOutputPath(conf: ExecutionConfiguration): String = {
    s"${conf.outputPath}/${conf.country.extension}/${conf.year}"
  }

  def fromSparkSubmit(args: Array[String]): ExecutionConfiguration = {
    ExecutionConfiguration(args(0), Country(args(1), Configuration.Countries(args(1))), args(2), args(3))
  }
}
