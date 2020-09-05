package com.atguigu.sparkcore.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark_Action_Agreegate
 * @Description: TODO  行动算子  agreegate聚合以集合外的一个元素为基准
 * @Author: VmPerson
 * @Date: 2020/8/6  22:43
 * @Version: 1.0
 */
object Spark_Action_Agreegate {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Action_Agreegate"))

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)


    /**
     * 需要注意： aggregate和aggregateByKey不同点在于a
     * ggregate的基参即在分区内使用，也在分区间使用
     * 10+1 ，2 => 13
     * (10+13,17) => 40
     * 10+3， 4 => 17
     * aggregateByKey只在分区内使用基础参数，分区间是不使用的
     */
    val res: Int = rdd.aggregate(10)(_ + _, _ + _)
    println("aggreagte结果为： " + res)

    //当分区间与分区内计算规则一致时： 使用fold代替aggregate
    val res1: Int = rdd.fold(10)(_ + _)
    println("fold结果为： " + res1)


    sc.stop()


  }


}
