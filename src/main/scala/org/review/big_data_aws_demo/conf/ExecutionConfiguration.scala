package org.review.big_data_aws_demo.conf

case class ExecutionConfiguration(
                                   inputPath: String,
                                   country: Country,
                                   outputPath: String,
                                   year: String
                                 )

case class Country(extension: String, label: String)

object Configuration {

  val Countries = Map("MG" -> "Madagascar",
    "FR" -> "France",
    "ES" -> "Spain",
    "MX" -> "Mexico",
    "US" -> "United States",
    "MA" -> "Morocco")

  def getCountry(extension: String): String = {
    assert(Countries.contains(extension), s"Country with extension ${extension} is missing")
    Countries(extension)
  }
}

