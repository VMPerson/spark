package com.atguigu.sparkcore.day07

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @ClassName: Spark_Item_One
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/8  13:47
 * @Version: 1.0
 */
object Spark_Item_Question_One_Five {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Item_Question_One_Five"))
    val fileRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    val my: MyCategorySum = new MyCategorySum
    //注册累加器
    sc.register(my, "MyCategorySum")


    fileRDD.foreach(action => {
      val line: Array[String] = action.split("_")
      if (line(6) != "-1") {
        my.add(line(6), "click")
      } else if (line(8) != "null") {
        val ids: Array[String] = line(8).split(",")
        ids.foreach(id => {
          my.add((id, "order"))
        }
        )
      } else if (line(10) != "null") {
        val ids: Array[String] = line(10).split(",")
        ids.foreach(id => {
          my.add((id, "pay"))
        })
      }
    })


    val mmp: mutable.Map[String, MySumCalculate] = my.value
    mmp.toList.sortBy(_._2).take(10).foreach(println)

    sc.stop()
  }


  class MyCategorySum extends AccumulatorV2[(String, String), mutable.Map[String, MySumCalculate]] {

    private val mySumCalculate = mutable.Map[String, MySumCalculate]()

    override def isZero: Boolean = mySumCalculate.isEmpty

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, MySumCalculate]] = new MyCategorySum

    override def reset(): Unit = mySumCalculate.clear()

    override def add(v: (String, String)): Unit = {
      val categoryId: String = v._1
      val actionType: String = v._2

      val calculate: MySumCalculate = mySumCalculate.getOrElse(categoryId, MySumCalculate(categoryId, 0, 0, 0))

      actionType match {
        case "click" => calculate.clickTimes += 1
        case "order" => calculate.orderTimes += 1
        case "pay" => calculate.payTimes += 1
      }
      mySumCalculate.update(categoryId, calculate)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, MySumCalculate]]): Unit = {
      other.value.foreach {
        case (id, otherCategory) => {
          val calculate: MySumCalculate = mySumCalculate.getOrElse(id, MySumCalculate(id, 0, 0, 0))
          calculate.clickTimes += otherCategory.clickTimes
          calculate.orderTimes += otherCategory.orderTimes
          calculate.payTimes += otherCategory.payTimes
          mySumCalculate.update(id, calculate)
        }
      }
    }

    override def value: mutable.Map[String, MySumCalculate] = mySumCalculate
  }


}


case class MySumCalculate(var categoryId: String, var clickTimes: Long, var orderTimes: Long, var payTimes: Long) extends Ordered[MySumCalculate] {

  override def compare(that: MySumCalculate): Int =
    if (clickTimes > that.clickTimes) {
      -1
    } else if (clickTimes == that.clickTimes) {
      if (orderTimes > that.orderTimes) {
        -1
      } else if (orderTimes == that.orderTimes) {
        if (payTimes > that.payTimes) {
          -1
        } else {
          1
        }
      } else {
        1
      }
    } else {
      1
    }


}


