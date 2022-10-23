package spark_core
package com.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

import scala.collection.mutable
object UDFMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("DailyUVFunction")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    //val avgVector64 = new UdfMapp
    val avgVector64 = new UdfMappMap
    val res=Seq(("A",Map(2->1,3->4)),("A",Map(2->5,3->4,1->3)),
      ("A",Map(2->1,3->4,5->6)),("B",Map(2->1,3->4)),("B",Map(2->5,3->4,1->3)),("B",Map(2->1,3->4,5->6))).toDF("flag","h")
    res.groupBy("flag").agg(avgVector64(col("h"))).show(false)
  }
}



