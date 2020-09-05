package com.atguigu.sparkcore.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Operator_LogFilter {


  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_LogFilter"))
    //读取日志文件、
    val logFile: RDD[String] = sc.textFile("datas/apache.log")

    //对数据进行处理
    val arr: RDD[String] = logFile.map(_.split(" ")(6))

    arr.collect().foreach(println)

    sc.stop()

  }


}
