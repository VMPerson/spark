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
object Spark_Item_Question_One_Four {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Item_Question_One_Four"))
    val fileRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    val rdd: RDD[(String, (Int, Int, Int))] = fileRDD.flatMap(
      elem => {
        var tmp: Array[String] = elem.split("_")
        if (tmp(6) != "-1") {
          List((tmp(6), (1, 0, 0)))
        } else if (tmp(8) != "null") {
          val ids: Array[String] = tmp(8).split(",")
          ids.map((_, (0, 1, 0)))
        } else if (tmp(10) != "null") {
          val ids: Array[String] = tmp(10).split(",")
          ids.map((_, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    val mmp: RDD[(String, (Int, Int, Int))] = rdd.reduceByKey {
      case ((x1, x2, x3), (y1, y2, y3)) => {
        (x1 + y1, x2 + y2, x3 + y3)
      }
    }

    val res: Array[(String, (Int, Int, Int))] = mmp.sortBy(_._2, false).take(10)
    res.foreach(println)

    sc.stop()
  }


}
