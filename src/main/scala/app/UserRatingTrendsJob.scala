package app

import model._

object UserRatingTrendsJob extends BaseSparkApp {

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    if(args.nonEmpty) {
      projectPath = args(0)
    }

    lazy val user = loadData[User]("user.json")
    lazy val usersWithFriends = user filter {
      _.friends.nonEmpty
    }
    usersWithFriends.cache
    val targetedUsers = usersWithFriends.map(_.user_id).collect
    lazy val reviews = loadData[Review]("review.json").filter(r => targetedUsers.contains(r.user_id))

    val avgStars = reviews
      .groupByKey(r => r.user_id)
      .mapGroups { case (id, res) =>
        (id, res.toSeq
          .groupBy(_.business_id)
          .mapValues(r => r.map(_.stars).sum.toDouble / (r.size.toDouble + 1))
          .map(identity))
      }.rdd.collectAsMap.toMap
    lazy val friends = usersWithFriends.rdd.map(u => (u.user_id, u.friends))
    val diffFromFriends = friends map { case (id, friends) =>
      val selfReviews = avgStars.getOrElse(id, Map.empty[String, Double])
      val friendReviews:Map[String, Double] = friends map { f =>
        avgStars.getOrElse(f, Map.empty[String, Double])
      } reduce { (a, b) =>
        (a.keySet ++ b.keySet map { k =>
          var sum = 0d
          var count = 0d
          if (a.isDefinedAt(k)) {
            sum += a(k)
            count += 1
          }
          if (b.isDefinedAt(k)) {
            sum += b(k)
            count += 1
          }
          (k, sum / count)
        }).toMap
      }
      id -> (selfReviews map { case (bId, selfR) =>
        (bId, math.abs(selfR - friendReviews.getOrElse(bId, selfR)) / selfR)
      }).mkString(",")
    }
    diffFromFriends.saveAsTextFile(args(1))


    //do your friends rate similar to you
    //does the number of friends you have correlate to how valueful are you as a user
  }

}
