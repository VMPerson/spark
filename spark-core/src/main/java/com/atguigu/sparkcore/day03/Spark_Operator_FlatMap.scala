package com.atguigu.sparkcore.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Operator_FlatMap {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_FlatMap"))

    val list: List[String] = List("Hello Spark", "Hello Scala", "Java Hadoop")

    val listRdd: RDD[String] = sc.makeRDD(list)


    //讲一个整体拆分成一个一个的个体来使用，称之为扁平化操作
    //  传参： 一个一个的数据
    //  返回参数： 可迭代数据类型

    val res: RDD[String] = listRdd.flatMap((str: String) => {
      str.split(" ")
    })
    res.collect().foreach(println)
    sc.stop()

  }


}
