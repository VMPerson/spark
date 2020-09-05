package com.atguigu.sparkcore.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark_Operator_Collection
 * @Description: TODO  关联算子
 * @Author: VmPerson
 * @Date: 2020/8/6  21:07
 * @Version: 1.0
 */
object Spark_Operator_Collection {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_Collection"))

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 88),  ("m", 95), ("n", 98)), 2)
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("c", 10), ("d", 11), ("a", 88)), 2)


    /**
     * join on :  将数据源中相同key的value聚合在一起，形成value元祖
     * 如果过没有匹配到相同的key则无法连接
     * join会产生笛卡尔积，数据成几何形增长，性能非常的低，能不用就不用
     */
  // val res: RDD[(String, (Int, Int))] = rdd1.join(rdd2, 2)
  //  val res: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2,1)
  //  val res: RDD[(String, (Option[Int], Int))] = rdd1.rightOuterJoin(rdd2)

    val res: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)


    res.collect().foreach(println)
    sc.stop()


  }


}
