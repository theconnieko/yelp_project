import java.sql.Date

package object model {

  case class User(user_id: String, name: String, review_count: BigInt, yelping_since: Date, friends: Seq[String],
                  useful: BigInt, funny: BigInt, cool: BigInt, fans: BigInt, elite: Seq[BigInt],
                  average_stars: Double, compliment_hot: BigInt, compliment_more: BigInt, compliment_profile: BigInt, compliment_cute: BigInt,
                  compliment_list: BigInt, compliment_note: BigInt, compliment_plain: BigInt, compliment_cool: BigInt, compliment_funny: BigInt,
                  compliment_writer: BigInt, compliment_photos: BigInt)

  case class Review(review_id: String, user_id: String, business_id: String, stars: BigInt,
                    date: Date, text: String, useful: BigInt, funny: BigInt, cool: BigInt)

  case class Business(business_id: String, name: String, address: String, city: String, state: String, postal_code: String,
                      latitude: Option[Double], longitude: Option[Double],
                      stars: Double, review_count: BigInt, is_open: BigInt, attributes: String,
                      categories: Seq[String], hours: Day)

  case class Tip(text: String, date: Date, business_id: String, user_id: String, likes: BigInt)

  case class CheckIn(business_id: String, time: Day)
  case class Day(Monday:Option[String], Tuesday:Option[String], Wednesday:Option[String], Thursday:Option[String],
                 Friday:Option[String], Saturday:Option[String], Sunday:Option[String])

}
