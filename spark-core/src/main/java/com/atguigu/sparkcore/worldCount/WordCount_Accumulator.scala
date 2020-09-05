package com.atguigu.sparkcore.worldCount

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @ClassName: WordCount_Accumulator
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/12  20:32
 * @Version: 1.0
 */
object WordCount_Accumulator {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("WordCount_Accumulator"))

    val fileRdd: RDD[String] = sc.textFile("datas/1.txt")

    val rdd: RDD[(String, Int)] = fileRdd.flatMap(_.split(" ")).map((_, 1))

    val mapRDD: RDD[mutable.Map[String, Int]] = rdd.map(kv => {
      mutable.Map[String, Int](kv)
    })

    //注册累加器
    val accumulator: MyAccumulator = new MyAccumulator
    sc.register(accumulator, "myAccumulator")

    mapRDD.foreach(elem => {
      accumulator.add(elem)
    })

    println(accumulator.value)

    sc.stop()
  }


}

class MyAccumulator extends AccumulatorV2[mutable.Map[String, Int], mutable.Map[String, Int]] {

  private val map: mutable.Map[String, Int] = mutable.Map[String, Int]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[mutable.Map[String, Int], mutable.Map[String, Int]] = new MyAccumulator

  override def reset(): Unit = map.clear()

  //区内逻辑
  override def add(v: mutable.Map[String, Int]): Unit = {
    map.foldLeft(v)((map1, map2) => {
      val k: String = map2._1
      val value: Int = map2._2
      val newValue = map1.getOrElse(k, 0) + value
      map1.update(k, newValue)
      null
    })
  }

  //区间逻辑
  override def merge(other: AccumulatorV2[mutable.Map[String, Int], mutable.Map[String, Int]]): Unit = {

    val otherMap: Map[String, Int] = other.value.toMap
    map.foldLeft(otherMap)((map1, map2) => {
      val k: String = map2._1
      val v: Int = map2._2
      var newVlue = map1.getOrElse(k, 0) + v
      map1.updated(k, newVlue)
    })
  }

  override def value: mutable.Map[String, Int] = map
}