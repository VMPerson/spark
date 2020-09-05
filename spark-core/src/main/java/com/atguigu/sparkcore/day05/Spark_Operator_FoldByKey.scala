package com.atguigu.sparkcore.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark_Operator_FoldByKey
 * @Description: TODO FoldByKey 算子
 * @Author: VmPerson
 * @Date: 2020/8/6  17:43
 * @Version: 1.0
 */
object Spark_Operator_FoldByKey {


  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_FoldByKey"))
    val rdd = sc.makeRDD(
      List(
        ("a", 1), ("a", 5), ("b", 2),
        ("a", 3), ("b", 4), ("b", 1)
      ),
      2
    )

    //foldByKey算子 当区间与区内计算逻辑相同时，又需要和集合外的元素做聚合时使用
    val res: RDD[(String, Int)] = rdd.foldByKey(0)(_ + _)

    res.collect().foreach(println)

    sc.stop()

  }


}
