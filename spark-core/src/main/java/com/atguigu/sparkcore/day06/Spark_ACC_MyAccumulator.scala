package com.atguigu.sparkcore.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark_ACC_My
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/7  22:46
 * @Version: 1.0
 */
object Spark_ACC_MyAccumulator {

  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_ACC_MyAccumulator"))

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6))
    /*
    //创建自定义累加器
    val myAccumulator: MyAccumulator = new MyAccumulator
    //注册使用自定义累加器
    sc.register(myAccumulator, "sum")
    rdd.foreach(elem => {
      myAccumulator.add(elem)
    })
    println("求和结果为： " + myAccumulator.value)
    */

    val aVG: MyAccumulatorAVG = new MyAccumulatorAVG
    sc.register(aVG, "avg")

    rdd.foreach(elem => {
      aVG.add(elem)
    })
    println("平均值结果为： " + aVG.value._1 / aVG.value._2)


    sc.stop()
  }
}

/**
 * 自定义累加器 求和
 */
class MyAccumulator extends AccumulatorV2[Int, Int] {
  private var isInit: Int = 0

  override def isZero: Boolean = isInit == 0

  override def copy(): AccumulatorV2[Int, Int] = {
    new MyAccumulator
  }

  override def reset(): Unit = isInit = 0

  override def add(v: Int): Unit = {
    isInit += v
  }

  override def merge(other: AccumulatorV2[Int, Int]): Unit = {
    isInit += other.value
  }

  override def value: Int = isInit
}


/**
 * 自定义累加器   求平均值
 */
class MyAccumulatorAVG extends AccumulatorV2[Int, (Int, Int)] {
  private var kk = 0
  private var vv = 0

  override def isZero: Boolean = kk == 0

  override def copy(): AccumulatorV2[Int, (Int, Int)] = new MyAccumulatorAVG

  override def reset(): Unit = {
    kk = 0
    vv = 0
  }

  override def add(v: Int): Unit = {
    kk += v
    vv += 1
  }

  override def merge(other: AccumulatorV2[Int, (Int, Int)]): Unit = {
    kk += other.value._1
    vv += other.value._2
  }

  override def value: (Int, Int) = (kk, vv)
}
