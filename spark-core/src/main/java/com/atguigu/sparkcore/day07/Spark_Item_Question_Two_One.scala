package com.atguigu.sparkcore.day07

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @ClassName: Spark_Item_Question_Two_One
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/9  21:41
 * @Version: 1.0
 */
object Spark_Item_Question_Two_One {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Item_Question_Two_One"))

    val fileRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")


    //注册累加器
    val sum: CategorySum = new CategorySum
    sc.register(sum, "categorySum")

    //先求出Top10热门商品
    fileRDD.foreach(elem => {
      val line: Array[String] = elem.split("_")
      if (line(6) != "-1") {
        sum.add(line(6), "click")
      } else if (line(8) != "null") {
        val ids: Array[String] = line(8).split(",")
        ids.foreach {
          id => {
            sum.add(id, "order")
          }
        }
      } else if (line(10) != "null") {

        val ids: Array[String] = line(10).split(",")
        ids.foreach {
          id => {
            sum.add(id, "pay")
          }
        }
      }
    })

    val tmp: mutable.Map[String, HotCategory] = sum.value

    val questionRes: List[(String, HotCategory)] = tmp.toList.sortBy(_._2).take(10)

    //Top10热门品类中每个品类的Top10活跃Session统计
    val map: Map[String, HotCategory] = questionRes.toMap

    val rdd: RDD[String] = fileRDD.filter(elem => {
      val line: Array[String] = elem.split("_")
      line(6) != "-1" && map.get(line(6)).isDefined
    })
    val mapRdd: RDD[((String, String), Int)] = rdd.map(elem => {
      val line: Array[String] = elem.split("_")
      ((line(6), line(2)), 1)
    })

    val sessionAndIdRDD: RDD[((String, String), Int)] = mapRdd.reduceByKey(_ + _)
    val IdRdd: RDD[(String, (String, Int))] = sessionAndIdRDD.map(elem => {
      (elem._1._1, (elem._1._2, elem._2))
    })

    val groupRdd: RDD[(String, Iterable[(String, Int)])] = IdRdd.groupByKey()
    val res: RDD[(String, List[(String, Int)])] = groupRdd.map(elem => {
      val tmp: List[(String, Int)] = elem._2.toList.sortWith((x, y) => {
        x._2 > y._2
      }).take(10)
      (elem._1, tmp)
    })

    res.foreach(println)

    sc.stop()
  }


  class CategorySum extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {

    private var hotCategory = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = hotCategory.isEmpty

    override def reset(): Unit = hotCategory.clear()

    override def add(v: (String, String)): Unit = {
      val categoryId: String = v._1
      val actionType: String = v._2
      val newCategory: HotCategory = hotCategory.getOrElse(categoryId, HotCategory(categoryId, 0, 0, 0))
      actionType match {
        case "click" => {
          newCategory.clickTimes += 1
        }
        case "order" => {
          newCategory.orderTimes += 1
        }
        case "pay" => {
          newCategory.payTimes += 1
        }
      }
      hotCategory.update(categoryId, newCategory)
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = new CategorySum

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      other.value.foreach {
        case (id, other) => {
          val category: HotCategory = hotCategory.getOrElse(id, HotCategory(id, 0, 0, 0))
          category.clickTimes += other.clickTimes
          category.orderTimes += other.orderTimes
          category.payTimes += other.payTimes
          hotCategory.update(id, category)
        }
      }
    }

    override def value: mutable.Map[String, HotCategory] = hotCategory
  }

}

case class HotCategory(var categoryId: String, var clickTimes: Long, var orderTimes: Long, var payTimes: Long) extends Ordered[HotCategory] {
  override def compare(that: HotCategory): Int = {
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
}