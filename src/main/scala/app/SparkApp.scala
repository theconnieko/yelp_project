package app

import app.MetricJob.projectPath
import model._

object SparkApp extends BaseSparkApp {

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    if(args.nonEmpty) {
      projectPath = args(0)
    }

    println("test")

    lazy val business = loadData[Business]("business.json")
    //    business.rdd.map(_.is_open).countByValue foreach println
    //    (closed,27865)
    //    (open,146702)
    //    println(business.count) //174567
    //    business.flatMap(_.categories).distinct().coalesce(1).write.text("categories")
    //    println((business filter { _.categories.contains("Food")}).count) //24777
    //    business.keyBy(b => BigDecimal(b.stars * (1 - math.pow(1/b.review_count.toDouble, 2))).setScale(1, BigDecimal.RoundingMode.HALF_UP)).combineByKey[Int](
    //      (_: Business) => 1,
    //      (a: Int, b: Business) => a + 1,
    //      (a: Int, b: Int) => a + b
    //    ).collect.toSeq.sortBy(_._1).foreach(s => println(s._1 + "," + s._2))

    //    business.groupByKey(_.stars)

    lazy val user = loadData[User]("user.json")
    //    println(user.count) //1326101
    lazy val usersWithFriends = user filter {
      _.friends.nonEmpty
    } //760008
    //    usersWithFriends.cache()
    lazy val count = usersWithFriends.count
    //    println(count)
    lazy val sumFriends = usersWithFriends map {
      _.friends.size
    } reduce {
      _ + _
    }
    //    println(sumFriends / count.toDouble) //65.298

    val targetedUsers = usersWithFriends.map(_.user_id).collect
    lazy val reviews = loadData[Review]("review_test.json").filter(r => targetedUsers.contains(r.user_id))
    //.collect
    //    lazy val dates = reviews.rdd.map(r => new LocalDate(r.date.getTime)).collect.toSeq.sorted(Ordering[LocalDate])
    //    println(dates.head) //2004-07-22
    //    println(dates.last) //2017-12-11
    //    println(reviews.count) //5261669
    //    println((reviews map { _.businessId }).distinct.count) //174567 business has a review
    //    println((reviews map { _.userId}).distinct.count) //1326101
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
      selfReviews map { case (bId, selfR) =>
        (selfR - friendReviews.getOrElse(bId, selfR)) / selfR
      }
    }
    diffFromFriends.collect foreach println
    //    reviews.groupByKey(_.user_id) mapGroups { (id, review) =>
    //      val friends = friendsMap.getOrElse(id, Seq.empty[String])
    //      friends
    //    }


    lazy val tips = loadData[Tip]("tip.json")
    //    println((tips map { _.business_id}).distinct.count) //112366
    //    println((tips map { _.user_id}).distinct.count) //271680
    //    println(tips.count)//1098325

    lazy val checkins = loadData[CheckIn]("checkin.json")
    //    println(checkins.count)

    //do your friends rate similar to you
    //does the number of friends you have correlate to how valueful are you as a user
  }

}
