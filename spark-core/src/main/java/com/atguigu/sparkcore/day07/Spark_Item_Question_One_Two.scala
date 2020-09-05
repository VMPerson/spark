package com.atguigu.sparkcore.day07

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark_Item_Question_One_Two
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/8  16:29
 * @Version: 1.0
 */
object Spark_Item_Question_One_Two {

  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Item_Question_One_Two"))

    val fileRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    //用leftoutjoin合适吗  不合适，使用cogroup试试


    val clickActionRDD: RDD[String] = fileRDD.filter(elem => {
      val tmp: Array[String] = elem.split("_")
      tmp(6) != "-1"
    })
    val orderActionRDD: RDD[String] = fileRDD.filter(elem => {
      val tmp: Array[String] = elem.split("_")
      tmp(8) != "null"
    })
    val payActionRDD: RDD[String] = fileRDD.filter(elem => {
      val tmp: Array[String] = elem.split("_")
      tmp(10) != "null"
    })

    val clickRDD: RDD[(String, Int)] = clickActionRDD.map(elem => {
      val tmp: Array[String] = elem.split("_")
      (tmp(6), 1)
    })
    val click: RDD[(String, Int)] = clickRDD.reduceByKey(_ + _)
    val orderRDD: RDD[(String, Int)] = orderActionRDD.flatMap(elem => {
      val tmp: Array[String] = elem.split("_")
      val ids: Array[String] = tmp(8).split(",")
      ids.map((_, 1))
    })
    val order: RDD[(String, Int)] = orderRDD.reduceByKey(_ + _)

    val payRDD: RDD[(String, Int)] = payActionRDD.flatMap(elem => {
      val tmp: Array[String] = elem.split("_")
      val ids: Array[String] = tmp(10).split(",")
      ids.map((_, 1))
    })
    val pay: RDD[(String, Int)] = payRDD.reduceByKey(_ + _)
    val clickCoOrder: RDD[(String, (Iterable[Int], Iterable[Int]))] = click.cogroup(order)

    val tmp: RDD[(String, (Int, Int))] = clickCoOrder.map {
      case (id, (click, order)) => {
        (id, (click.head, order.head))
      }
    }

    val clickCoOrderCoPay: RDD[(String, (Iterable[(Int, Int)], Iterable[Int]))] = tmp.cogroup(pay)

    val res: RDD[(String, (Int, Int, Int))] = clickCoOrderCoPay.map {
      case (id, (left, right)) => {
        val (clickCount, orderCount) = left.head
        val payCount: Int = right.head
        (id, (clickCount, orderCount, payCount))
      }
    }
    val tuples: Array[(String, (Int, Int, Int))] = res.sortBy(_._2, false).take(10)
    tuples.foreach(println)
    sc.stop()

  }


}
