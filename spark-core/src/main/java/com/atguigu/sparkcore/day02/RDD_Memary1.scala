package com.atguigu.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Memary1 {

  def main(args: Array[String]): Unit = {

    //TODO 建立和spark环境的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("Create_RDD_Memary")
    val sc: SparkContext = new SparkContext(sparkConf)


    val list: List[Int] = List(1, 2, 3, 4, 5)

    //通过paralelize方式创建RDD在代码层面不能体现其并行性，我们可以采用mkRdd的方式
    //mkrdd方式，底层还是调用paralelize的方式创建的
    var res: RDD[Int] = sc.makeRDD(list)
    var resMap: RDD[Int] = res.map(num => {
      println("map........")
      num * 2
    })


    //在没有调用collect方法之前，上面的方法是不执行的,这便体现了延时加载的概念
    resMap.collect().foreach(println)

    sc.stop()


  }


}
