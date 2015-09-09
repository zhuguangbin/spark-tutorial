package com.mvad.spark.demo.dataframe

/**
 * Created by zhugb on 15-9-9.
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object DspLogParquetDemo {

  def main(args: Array[String]) {

    val sc = new SparkContext(
      new SparkConf()
        .setAppName("Spark:[demo][DataFrameUsingDspLogParquet]"))
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import sqlContext.implicits._

    // Create a DataFrame from Parquet files
    val df = sqlContext.read.parquet("/mvad/warehouse/ods/dsp/date=2015-09-01/hour=00/type=*")

    val result = df
      .select("request.geo.province","request.userAgentInfo.os","request.userAgentInfo.browser")
      .groupBy("province", "os", "browser").count()

    result.show()

    result.write.format("parquet").mode("overwrite").save("/tmp/dsprequest")

    sc.stop()
  }

}
