package com.atguigu.sparkcore.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Operator_AggregateByKey {

  def main(args: Array[String]): Unit = {

    //aggregateByKey 是对key相同的进行value聚合
    //reduceBykey也是对key相同的进行value的聚合， 只不过区间和区内排序规则是一样的
    // 某些业务需求，区内和区间排序不要求一样此时我们便要是用aggregateByKey来处理了
    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_AggregateByKey"))
    val rdd = sc.makeRDD(
      List(
        ("a", 1), ("a", 5), ("b", 2),
        ("a", 3), ("b", 4), ("b", 1)
      ),
      2
    )

    //arrregateByKey参数采取的是柯力化函数
    // 有两个参数 第一个参数：是分区内计算的初始值
    //       第二个参数又有两个参数：
    //              第一个：分区内规则
    //              第二个：是分区间规则


    val res: RDD[(String, Int)] = rdd.aggregateByKey(0)(
      (x, y) => {
        math.max(x, y)
      },
      (x, y) => {
        x + y
      }
    )
    res.collect().foreach(println)


  }


}
