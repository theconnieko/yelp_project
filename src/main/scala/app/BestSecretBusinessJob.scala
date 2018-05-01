package app

import model.{Business, Tip}

object BestSecretBusinessJob extends BaseSparkApp {

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    if (args.nonEmpty) {
      projectPath = args(0)
    }
    val keyword = args(2)

    lazy val business = loadData[Business]("business.json").rdd.keyBy(_.business_id)
    val tip = loadData[Tip]("tip.json").rdd.filter(_.text.toLowerCase.contains(keyword.toLowerCase())).keyBy(_.business_id)

    val result = business.cogroup(tip) filter { case(_, (b, t)) =>
      b.filter(_.categories.map(_.toLowerCase).contains(keyword.toLowerCase())).nonEmpty ||
      t.nonEmpty
    } flatMap {
      _._2._1 map { b =>
        ((b.city, b.state), (b.business_id, b.stars))
      }
    } groupByKey() mapValues  { v =>
      v.toSeq.sortBy(_._2)(Ordering[Double].reverse).take(10)
    }

    result.coalesce(1).saveAsTextFile(args(1))

  }
}
