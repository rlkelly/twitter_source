package org.apache.spark.sql.brainshare

import java.lang.Integer.MIN_VALUE
import org.json4s.JValue
import org.json4s.jackson.JsonMethods._

import scalaj.http.{Http, HttpOptions}
import scala.collection.mutable.ListBuffer

case class TweetData(screen_name: String, likes: Int, follower_count: Int)


object TweetBot {
  def getTweets(since_id: BigInt, count: Int) : (ListBuffer[TweetData], BigInt) = {
    val basic = sys.env("TWITTER_TOKEN")
    implicit val formats = org.json4s.DefaultFormats

    val token = Http("https://api.twitter.com/oauth2/token?grant_type=client_credentials")
      .header("Authorization", s"Basic ${basic}")
      .header("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8")
      .header("Content-Length", "29")
      .postForm
      .param("grant_type", "client_credentials")
      .option(HttpOptions.readTimeout(3000)).asString
      .body
    val bearer = token.split("\"")(7)

    println(s" QUERY URL: https://api.twitter.com/1.1/search/tweets.json?q=%23hpe&count=${count}&since_id=${since_id}")
    val result = Http(s"https://api.twitter.com/1.1/search/tweets.json?q=%23hpe&count=${count}&since_id=${since_id}")
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .header("Authorization", s"Bearer ${bearer}")
      .option(HttpOptions.readTimeout(3000)).asString
      .body

    val tweetList = ListBuffer.empty[TweetData]

    val json = parse(result).extract[Map[String, Any]]
    val status = json("statuses").asInstanceOf[List[Map[String, Any]]]
    println(s"status length: ${status.length}")
    for (row <- status) {
      val user = row("user").asInstanceOf[Map[String, Any]]
      val screen_name = user("screen_name").asInstanceOf[String]
      // val tweet = row("text").asInstanceOf[String]
      val follower_count = user("followers_count").asInstanceOf[BigInt].toInt
      val likes = row("favorite_count").asInstanceOf[BigInt].toInt
      tweetList.append(TweetData(screen_name, likes, follower_count))
    }
    println()
    val last = status.last("id").asInstanceOf[BigInt]
    (tweetList, last)
  }
}
