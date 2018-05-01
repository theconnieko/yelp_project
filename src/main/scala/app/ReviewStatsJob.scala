package app

import model.{Business, Review}

object ReviewStatsJob extends BaseSparkApp {

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    if (args.nonEmpty) {
      projectPath = args(0)
    }
    val reviews = loadData[Review]("review.json").rdd.keyBy(_.business_id)
    val business = loadData[Business]("business.json").rdd.keyBy(_.business_id)

    val grouped = reviews.cogroup(business) filter { case (_, (r, b)) =>
      r.nonEmpty && b.nonEmpty
    }
    grouped.cache()

    lazy val reviewsPerCity = grouped map { case (_, (r, b)) =>
      ((b.head.city, b.head.state), r.size)
    } reduceByKey {
      _ + _
    }
    reviewsPerCity.sortBy(_._2).coalesce(1).saveAsTextFile(args(1) + "reviewsPerCity")

    lazy val reviewsPerCategory = grouped flatMap { case (_, (r, b)) =>
      b.head.categories map { cat =>
        cat -> r.size
      }
    } reduceByKey {
      _ + _
    }
    reviewsPerCategory.sortBy(_._2).coalesce(1).saveAsTextFile(args(1) + "reviewsPerCategory")

    val cat = grouped flatMap { case (_, (r, b)) =>
      b.head.categories map { cat =>
        ((b.head.city, b.head.state, cat), r.size)
      }
    } reduceByKey {
      _ + _
    } map { case((city, state, cat), count) =>
      ((city, state), (cat, count))
    }
    val topCatPerCity = cat.groupByKey map { case(k, v) =>
      k -> v.toSeq.sortBy(_._2)(Ordering[Int].reverse).head
    }
    topCatPerCity.sortByKey().coalesce(1).saveAsTextFile(args(1) + "MostReviewedCategoryPerCity")

  }

}
