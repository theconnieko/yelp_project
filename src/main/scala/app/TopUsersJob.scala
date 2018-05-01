package app

import model.{Business, Review, Tip}

object TopUsersJob extends BaseSparkApp {

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    if (args.nonEmpty) {
      projectPath = args(0)
    }
    val tip = loadData[Tip]("tip.json").rdd
      .map(t => ((t.user_id, t.business_id), 1))
    val reviews = loadData[Review]("review.json").rdd
      .map(r => ((r.user_id, r.business_id), 1))
    val businesses = loadData[Business]("business.json").rdd
      .map(b => (b.business_id, (b.city, b.state)))

    val countedReviews = spark.sparkContext.union(reviews, tip)
      .reduceByKey(_+_)
      .map(r => (r._1._2, (r._1._1, r._2)))

    countedReviews
      .join(businesses)
      .map { case (bId, (r, add)) =>
        (add, (r._1, r._2))
      }
      .groupByKey()
      .mapValues(count => count
        .groupBy(_._1)
        .mapValues(v => v.map(_._2).sum)
        .toSeq
        .sortBy(_._2)(Ordering[Int].reverse).take(10).map(_._1).mkString(","))
      .saveAsTextFile(args(1)+"TopUser")

  }
}
