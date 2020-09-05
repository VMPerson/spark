package com.atguigu.sparkcore.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark_Operator_CombineByKey
 * @Description: TODO  CombineByKey算子
 * @Author: VmPerson
 * @Date: 2020/8/6  17:18
 * @Version: 1.0
 */
object   Spark_Operator_CombineByKey {


  //求每个key的平均值
  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_CombineByKey"))
    val list: List[(String, Int)] = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    val rdd: RDD[(String, Int)] = sc.makeRDD(list, 2)

    //combineByKey 聚合函数 参数列表传递三个参数
    //   第一个参数：  为了统计方便，计算式用到的第一个值进行结构的转换
    //   第二个参数：  分区内的计算规则
    //   第三个参数：  分区间的计算规则

    val comRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
      (_, 1),
      (acc: (Int, Int), y) => {
        (acc._1 + y, acc._2)
      },
      (acc1: (Int, Int), acc2: (Int, Int)) => {
        (acc1._1 + acc2._1, acc1._2 + acc2._2)
      }
    )

    val res: RDD[(String, Int)] = comRDD.map(data => {
      var tmp = data._2._1 / data._2._2
      (data._1, tmp)
    })

    res.collect().foreach(println)

    sc.stop()


  }


}
