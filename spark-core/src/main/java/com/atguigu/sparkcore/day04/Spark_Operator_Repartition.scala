package com.atguigu.sparkcore.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Operator_Repartition {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_Repartition"))

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    //repartition增大分数数量，采用了shuffle
    //底层还是采用  coalesce(numPartitions, shuffle = true)
    val res: RDD[Int] = rdd.repartition(4)
    res.saveAsTextFile("output3")

    sc.stop()

  }


}
