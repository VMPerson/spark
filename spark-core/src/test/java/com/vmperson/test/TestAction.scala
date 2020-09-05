package com.vmperson.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: TestAction
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/6  23:49
 * @Version: 1.0
 */
object TestAction {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Action_Agreegate"))

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val res: Array[Int] = rdd.takeOrdered(2)
    res.foreach(println)

    sc.stop()

  }


}
