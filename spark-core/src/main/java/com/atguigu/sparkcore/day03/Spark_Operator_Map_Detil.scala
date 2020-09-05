package com.atguigu.sparkcore.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Operator_Map_Detil {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_Map_Detil"))

    //集合的方法只能成为方法
    val list: List[String] = List("Hello", "Scala", "Spark", "Java")

    //而RDD的方法和平常的方法不一样，因为RDD有分区的概念
    //我们以一个集合为例，设置两个分区
    val listRdd: RDD[String] = sc.makeRDD(list, 2)

    //逻辑上应该（hello,scala） （spark,java）
    val mapRdd: RDD[String] = listRdd.map(_ * 2)


    //由于没有对改变分区的数量进行操作，所以分区数量不会改变
    //由于一条数据一条数据的进行操作，不存在改变分区的操作，所以数据f所在分区也不会改变

    mapRdd.saveAsTextFile("output3")

    sc.stop()

  }


}
