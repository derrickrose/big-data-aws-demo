package org.review.big_data_aws_demo

trait SparkTestContext {

  import org.apache.spark.sql.SparkSession

  val TestSparkSession = SparkSession.builder()
    .appName("soccer games filtering test")
    .master("local[*]")
    //.config("spark.testing.memory", "2140000000")
    .config("spark.sql.caseSensitive", "true")
    .getOrCreate()
}
