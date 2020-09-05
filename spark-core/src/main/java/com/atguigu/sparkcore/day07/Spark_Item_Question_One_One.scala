package com.atguigu.sparkcore.day07

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark_Item_One
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/8  13:47
 * @Version: 1.0
 */
object Spark_Item_Question_One_One {

  def main(args: Array[String]): Unit = {


    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Item_Question_One_One"))

    val fileRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    //过滤非点击数据
    val clickActionRDD: RDD[String] = fileRDD.filter(elem => {
      var tmp: Array[String] = elem.split("_")
      tmp(6) != "-1"
      tmp(7) != "-1"
    })

    //获取点击数据
    val clickRDD: RDD[(String, Int)] = clickActionRDD.map(elem => {
      val tmp: Array[String] = elem.split("_")
      (tmp(6), 1)
    })
    val click: RDD[(String, Int)] = clickRDD.reduceByKey(_ + _)

    //过滤非下单数据
    val orderActionRDD: RDD[String] = fileRDD.filter(elem => {
      val tmp: Array[String] = elem.split("_")
      tmp(8) != "null"
    })

    //获取下单数据
    val orderRDD: RDD[(String, Int)] = orderActionRDD.flatMap(elem => {
      val tmp: Array[String] = elem.split("_")
      val ids: Array[String] = tmp(8).split(",")
      ids.map((_, 1))
    })
    val order: RDD[(String, Int)] = orderRDD.reduceByKey(_ + _)

    //过滤非支付数据
    val payActionRDD: RDD[String] = fileRDD.filter(elem => {
      val tmp: Array[String] = elem.split("_")
      tmp(10) != "null"
    })
    val payRDD: RDD[(String, Int)] = payActionRDD.flatMap(elem => {
      val tmp: Array[String] = elem.split("_")
      val ids: Array[String] = tmp(10).split(",")
      ids.map((_, 1))
    })
    val pay: RDD[(String, Int)] = payRDD.reduceByKey(_ + _)


    val clickCoOrder: RDD[(String, (Int, Option[Int]))] = click.leftOuterJoin(order)
    val clickCoOrderCoPay: RDD[(String, ((Int, Option[Int]), Option[Int]))] = clickCoOrder.leftOuterJoin(pay)
    val mapRDD: RDD[(String, (Int, Int, Int))] = clickCoOrderCoPay.map {
      case (id, ((click, order), pay)) => {
        (id, (click, order.getOrElse(0), pay.getOrElse(0)))
      }
    }
    val res: Array[(String, (Int, Int, Int))] = mapRDD.sortBy(_._2,false).take(10)
    res.foreach(println)


    sc.stop()
  }


}
