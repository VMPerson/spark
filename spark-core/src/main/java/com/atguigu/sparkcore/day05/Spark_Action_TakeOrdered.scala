package com.atguigu.sparkcore.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark_Action_TakeOrdered
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/6  23:52
 * @Version: 1.0
 */
object Spark_Action_TakeOrdered {


  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Action_TakeOrdered"))


    /**
     * 获取集合里面前n个元素作为元祖返回
     */
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val res: Array[Int] = rdd.takeOrdered(2)
    res.foreach(println)

    sc.stop()
  }


}
