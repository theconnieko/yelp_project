package app

import model.Business

object Best10Job extends BaseSparkApp {

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    if (args.nonEmpty) {
      projectPath = args(0)
    }

    lazy val business = loadData[Business]("business.json")

    val keyed = business.rdd.flatMap(b =>
      b.categories.map(c =>
        (b.city + "," + b.state, c, b.business_id, b.stars / math.pow(1 - b.review_count.toDouble, 2))))
      .keyBy(c => (c._1, c._2))
      .groupByKey()

    keyed.map{ case(k, rating) => k -> rating.toSeq.sortBy(_._4).take(10).map(_._3).mkString(",")}
      .sortBy(_._1._1)
      .map(k => s"${k._1._1},${k._1._2},${k._2}")
      .saveAsTextFile(args(1))
  }
}
