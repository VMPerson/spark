package com.atguigu.sparkcore.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark_Action_CountBy
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/6  23:07
 * @Version: 1.0
 */
object Spark_Action_CountBy {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Action_CountBy"))

    val rdd: RDD[String] = sc.makeRDD(List("spark", "hello hello", "spark", "hello scala", "hello spark"), 2)

    val rdd0: RDD[String] = rdd.flatMap(_.split(" "))

    val rdd1: RDD[(String, Int)] = rdd0.map((_, 1))


    /**
     * countByKey：行动算子，统计key的个数
     */


    /*    val res: collection.Map[String, Long] = rdd1.countByKey()
        res.foreach(println)*/


    /**
     * 统计元素的个数,这里的byValue并不是指 map的value 而是单列集合的值
     */


    val res: collection.Map[String, Long] = rdd0.countByValue()
    res.foreach(println)


    sc.stop()
  }


}
