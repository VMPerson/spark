package com.atguigu.sparkSql.day03

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
 * @ClassName: Spark_Que_Query_One
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/14  10:35
 * @Version: 1.0
 */
object Spark_Que_Query_Two {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Que_Query_Two")

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    val myUDAFFuncations: MyUDAFFuncations = new MyUDAFFuncations
    spark.udf.register("myudaf", functions.udaf(myUDAFFuncations))

    spark.sql("use spark")

    spark.sql(
      """
        |select a.*,
        |	   b.area,
        |    b.city_name,
        |	   c.product_name
        |from user_visit_action  a
        |join city_info  b on  a.city_id = b.city_id
        |join product_info c on a.click_product_id = c.product_id
        |  where a.click_product_id > -1
        |""".stripMargin).createOrReplaceTempView("t1")

    spark.sql(
      """
        |select
        |   area,  product_name, count(*) as clickCount , myudaf(city_name) as city_remark
        |   from
        | t1
        | group by area,product_name
        |""".stripMargin).createOrReplaceTempView("t2")

    spark.sql(
      """
        |select
        |	*,rank() over(partition by area order by clickCount desc) as top
        |from
        |t2
        |""".stripMargin).createOrReplaceTempView("t3")

    spark.sql(
      """
        |select  * from t3 where top<=3
        |""".stripMargin).show(false)

    spark.stop()

  }

  case class Buff(var totalCount: Long, var cityMap: mutable.Map[String, Long])


  class MyUDAFFuncations extends Aggregator[String, Buff, String] {

    override def zero: Buff = {
      Buff(0L, mutable.Map[String, Long]())
    }

    override def reduce(b: Buff, a: String): Buff = {
      b.totalCount += 1
      var newValue = b.cityMap.getOrElse(a, 0L) + 1
      b.cityMap.update(a, newValue)
      b
    }

    override def merge(b1: Buff, b2: Buff): Buff = {

       b1.totalCount += b2.totalCount
      val map1: mutable.Map[String, Long] = b1.cityMap
      val map2: mutable.Map[String, Long] = b2.cityMap


      b1.cityMap = map1.foldLeft(map2) {
        case (map, (k, v)) => {
          var newCnt = map.getOrElse(k, 0L) + v
          map.update(k, newCnt)
          map
        }
      }
      b1
    }

    override def finish(reduction: Buff): String = {

      var list = ListBuffer[String]()
      val totalCount: Long = reduction.totalCount
      val map: mutable.Map[String, Long] = reduction.cityMap

      val citylist: List[(String, Long)] = map.toList.sortWith((left, right) => {
        left._2 > right._2
      }).take(2)

      val isHasOther = map.size > 2
      var sum: Long = 0

      citylist.foreach { elem => {
        val r: Long = elem._2 * 100 / totalCount
        var str = elem._1 + " " + r + "%"
        sum += r
        list.append(str)
      }
      }

      if (isHasOther) {
        list.append("其他 " + (100 - sum) + "%")
      }
      list.mkString(",")

    }

    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }


}
