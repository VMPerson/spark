package com.atguigu.sparkcore.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Operator_Filter {

  def main(args: Array[String]): Unit = {


    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator_Filter"))

    //将数据源中的每一条数据，根据规则进行过滤，符合为true数据保留，不符合flase数据丢弃，数据量由多变少
    //rdd.filter(num=>true)  保留所有数据
    //rdd.filter(num=>flase) 删除所有数据


    /*    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

        val filterRdd: RDD[Int] = rdd.filter(_ % 2 == 0)

        filterRdd.collect().foreach(println)

        sc.stop()
    */
    //小需求： 从服务器日志数据apache.log中获取2015年5月17日的请求路径

    val file: RDD[String] = sc.textFile("datas/apache.log")

    val filterRdd: RDD[String] = file.filter(_.split(" ")(3).startsWith("17/05/2015"))

    filterRdd.collect().foreach(println)
    sc.stop()

  }


}
