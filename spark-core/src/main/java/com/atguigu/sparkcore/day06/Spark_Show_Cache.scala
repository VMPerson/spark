package com.atguigu.sparkcore.day06

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark_Show_Cache
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/7  22:27
 * @Version: 1.0
 */
object Spark_Show_Cache {


  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Show_Cache"))

    sc.stop()

  }
}
