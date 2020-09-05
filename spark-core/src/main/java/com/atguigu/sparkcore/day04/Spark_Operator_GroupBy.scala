package com.atguigu.sparkcore.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}



object Spark_Operator_GroupBy {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_GroupBy"))

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    val groupRdd: RDD[(Int, Iterable[Int])] = rdd.groupBy(_ % 2)

    groupRdd.collect foreach println

    sc.stop()
  }

}
