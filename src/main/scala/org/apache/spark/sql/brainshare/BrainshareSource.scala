package org.apache.spark.sql.brainshare

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.unsafe.types.UTF8String

import scala.util.Random
import java.util.concurrent._
import scala.collection.BitSet
import collection.mutable.ListBuffer

// inspired by https://hackernoon.com/spark-custom-stream-sources-ec360b8ae240


class BrainshareSource private (sqlContext: SQLContext, override val schema: StructType) extends Source {
  private val DEFAULT_OFFSET: LongOffset = LongOffset(0)
  private val MAX_BATCH_SIZE: Int = 50
  private val MAX_QUEUE_SIZE: Int = 500
  private val TWEET_COUNT: Int = 100

  private var since = BigInt(0)
  private var latest_offset = -1L
  private var current_offset = 0L
  private var last_sent = 0L


  private var queue = ListBuffer.empty[(Long, TweetData)]
  private val incrementalThread = dataGeneratorStartingThread()

  override def getOffset: Option[Offset] = this.synchronized {
    if (queue.length == 0) {
      None
    } else {
      val remaining_length = queue.last._1 - current_offset + 1
      val offset = LongOffset(last_sent + List(MAX_BATCH_SIZE, remaining_length).min)

      println(s"getOffset: $offset, ${queue.last._1}, ${current_offset}")

      Some(offset)
    }
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = this.synchronized {

    val s = start.flatMap(LongOffset.convert).getOrElse(DEFAULT_OFFSET).offset
    val e = LongOffset.convert(end).getOrElse(DEFAULT_OFFSET).offset
    last_sent = e

    println(s"generating batch range $start ; $end")

    val data = queue
      .par
      .filter { case (idx, _) => idx >= s && idx < e }
      .map { case (_, v) => v }
      .seq

    val rdd = sqlContext
      .sparkContext
      .parallelize(data)
      .map { case v => InternalRow(UTF8String.fromString(v.screen_name), v.likes, v.follower_count) }

    sqlContext.sparkSession.internalCreateDataFrame(rdd, schema, isStreaming = true)
  }

  override def commit(end: Offset): Unit = this.synchronized {

    val committed = LongOffset.convert(end).getOrElse(DEFAULT_OFFSET).offset
    val toKeep = queue.filter { case (idx, _) => idx >= committed }

    println(s"after clean size ${toKeep.length}")
    println(s"deleted: ${queue.size - toKeep.size}")
    println(s"committed ${committed}")

    queue = toKeep
    current_offset = committed
  }

  override def stop(): Unit = incrementalThread.stop()

  private def dataGeneratorStartingThread() = {
    val t = new Thread("increment") {
      setDaemon(true)
      override def run(): Unit = {

        while (true) {
          print(".")
          try {
            this.synchronized {
              if (queue.length < MAX_QUEUE_SIZE) {
                val (tweets: ListBuffer[TweetData], latest_id: BigInt) = TweetBot.getTweets(since, TWEET_COUNT)
                since = latest_id

                for (tweet <- tweets) {
                  latest_offset += 1
                  queue.append((latest_offset, tweet))
                }
              }
            }
          } catch {
            case e: Exception => println(e)
          }

          Thread.sleep(10000)
        }
      }
    }

    t.start()

    t
  }
}

object BrainshareSource {
  def apply(sqlContext: SQLContext, schema: Option[StructType]): Source = new BrainshareSource(sqlContext, schema.get)
}
