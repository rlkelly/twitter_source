package com.brainshare

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

object App {

  def main(args: Array[String]): Unit = {
    val sc = new org.apache.spark.SparkContext("local", "shell")
    val hadoopConf = sc.hadoopConfiguration;

    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopConf.set("fs.s3a.awsAccessKeyId", "AKIAUYGXRLQTATRVH3X5")
    hadoopConf.set("fs.s3a.awsSecretAccessKey", "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")

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val config = new org.apache.spark.SparkConf().setAppName("Brainshare App")

    val sparkSession = (SparkSession
      .builder()
      .config(config)
      .getOrCreate())

    sparkSession.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/check")
    sparkSession.conf.set("spark.sql.streaming.schemaInference", value = true)
    sparkSession.sparkContext.setLogLevel("WARN")

    val s = StructType(List(StructField("screen_name", StringType), StructField("follower_count", IntegerType), StructField("likes", IntegerType)))

    val inputDf = (sparkSession
      .readStream
      .format("BrainshareApp")
      .schema(s)
      .load())

    inputDf.createTempView("w")

    (sparkSession
      .sql("select screen_name, SUM(likes) as c from w group by screen_name order by c desc")
      .writeStream
      .outputMode("complete")
      .format("console")
      .start
      .awaitTermination)
  }
}
