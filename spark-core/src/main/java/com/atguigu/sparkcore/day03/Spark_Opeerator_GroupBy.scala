package com.atguigu.sparkcore.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Opeerator_GroupBy {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Opeerator_GroupBy"))


    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    //将数据源中的每一套数据按照指定规则进行分组
    //相同规则的数据会被分配到一个组里面

    val res: RDD[(Int, Iterable[Int])] = rdd.groupBy(_ % 2)
    res.collect().foreach(println)

    sc.stop()


  }


}
