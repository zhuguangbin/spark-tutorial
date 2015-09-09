/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mvad.spark.demo.streaming

import com.mediav.data.log.LogUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Usage:
 *    `$ bin/spark-submit --class \
 *      com.mvad.spark.demo.streaming.KafkaWordCount zk1dg,zk2dg,zk3dg:2181/ \
 *      my-consumer-group d.b.3,d.b.3.m 1`
 */
object KafkaWordCount {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    lines.foreachRDD((rdd: RDD[String], time: Time) => {
      // Get the singleton instance of SQLContext
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      // Convert RDD[String] to RDD[case class] to DataFrame
      val wordsDataFrame = rdd.map(base64Decode)
        .map(w => DSPLogInfo(w.getShowInfo.getAdspaceId, w.getShowInfo.getFee)).toDF()

      // Register as table
      wordsDataFrame.registerTempTable("dsplog")

      // Do word count on table using SQL and print it
      val wordCountsDataFrame =
        sqlContext.sql("select advertiserId,sum(fee) as totalfee from dsplog " +
          "group by advertiserId")
      println(s"========= $time =========")
      wordCountsDataFrame.show()
    })

    ssc.start()
    ssc.awaitTermination()
  }


  def base64Decode(base64Str:String) = {
    LogUtils.ThriftBase64decoder(base64Str, classOf[dsp.thrift.LogInfo]);
  }

  case class DSPLogInfo(advertiserId:Integer, fee:Integer )
}

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }

}
