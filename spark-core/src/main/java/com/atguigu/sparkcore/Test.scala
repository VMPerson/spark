package com.atguigu.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Test
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/11  13:58
 * @Version: 1.0
 */
object Test {


  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Item_Question_One_Five"))


    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd2: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd3: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    val res: RDD[Int] = rdd1.union(rdd2).union(rdd3)
    res.foreach(println)

    sc.stop()
  }


}
