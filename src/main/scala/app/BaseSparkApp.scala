package app

import org.apache.spark.sql.{Encoder, SparkSession}

trait BaseSparkApp {

  lazy val spark = SparkSession.builder.getOrCreate()

  lazy val sqlContext = spark.sqlContext

  var projectPath = "/Users/cko/Documents/cloudcomputing/yelp_project/dataset/"

  def loadData[T](file: String)(implicit encoder: Encoder[T]) = {
    spark.read.json(projectPath + file).as[T]
  }

}
