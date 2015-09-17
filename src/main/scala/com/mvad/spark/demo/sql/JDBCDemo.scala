package com.mvad.spark.demo.sql

/**
 * Created by zhugb on 15-5-15.
 */

import org.apache.spark.{SparkConf, SparkContext}

object JDBCDemo {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("SparkSQL:[demo][SparkSQLUsingMySQL]"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val props = new java.util.Properties()
    props.put("user","hadoop")
    props.put("password","RNymee2527#")

    val jdbcDF = sqlContext.read.jdbc("jdbc:mysql://adm1dg:3306/hadoop", "sparksql_history", props)
    jdbcDF.registerTempTable("sparksql_history")
    sqlContext.tableNames.foreach(println)
    jdbcDF.printSchema()
    jdbcDF.show(10)

    val sql = "select count(id) as runcount, retcode from sparksql_history group by retcode"
    val result = sqlContext.sql(sql)
    result.show()
    result.rdd.saveAsTextFile("/tmp/result-jdbc")

    // bug api, only create to default DB
    result.write.saveAsTable("result-jdbc")

    sc.stop()
  }

}
