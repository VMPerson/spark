package com.atguigu.sparkcore.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Operator_FlatMap_Detil {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_FlatMap_Detil"))


    val sep = sc.makeRDD(List(List(1, 2, 3), 5, 6, 7, List(8, 9, 10)))

    val res: RDD[Any] = sep.flatMap(d => {
      d match {
        case list: List[_] => list
        case _ => List(d)
      }
    })


    res.collect().foreach(println)
    sc.stop()

  }


}
