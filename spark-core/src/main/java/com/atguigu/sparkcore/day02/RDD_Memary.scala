package com.atguigu.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Memary {

  def main(args: Array[String]): Unit = {

    //TODO 建立和spark环境的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("Create_RDD_Memary")
    val sc: SparkContext = new SparkContext(sparkConf)


    val list: List[Int] = List(1, 2, 3, 4, 5)

    //parallelize方法是将集合里的数据作为数据源处理
    //parallelize方法可以创建RDD,并指明RDD里面处理的数据类型
    var res: RDD[Int] = sc.parallelize(list)

    var resMap: RDD[Int] = res.map(num => {
      println("map........")
      num * 2
    })


    //在没有调用collect方法之前，上面的方法是不执行的,这便体现了延时加载的概念
    resMap.collect().foreach(println)

    sc.stop()


  }


}
