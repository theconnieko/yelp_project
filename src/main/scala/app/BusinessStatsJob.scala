package app

import model.Business

object BusinessStatsJob extends BaseSparkApp {

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    if (args.nonEmpty) {
      projectPath = args(0)
    }

    val business = loadData[Business]("business.json").rdd

    val betterCompetitor = business.keyBy(b => (b.city, b.state)).groupByKey map { case (k, b) =>
      val competition = b.flatMap(bB => bB.categories.map(c => (c, bB)))
        .groupBy(_._1)
        .mapValues(itr => {
          val bS = itr.map(_._2)
          bS.map(bSS => itr
            .map(_._2)
            .filterNot(_ == bSS)
            .map(other => (bSS.business_id, ((if (bSS.stars < other.stars) 1 else 0), 1)))
            .groupBy(_._1)
            .mapValues(v => v.map(_._2).reduce((a,b) => (a._1+b._1, a._2+b._2)))
            .map(identity))
            .filter(_.nonEmpty)
        }).map(identity)
        .filter(_._2.nonEmpty)
      (k, competition)
    } filter { _._2.nonEmpty }

    betterCompetitor.coalesce(1).saveAsTextFile(args(1)+"betterCompetitor")

  }

}
