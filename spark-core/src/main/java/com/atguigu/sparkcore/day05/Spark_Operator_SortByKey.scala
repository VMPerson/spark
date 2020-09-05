package com.atguigu.sparkcore.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark_Operator_SortByKey
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/6  20:16
 * @Version: 1.0
 */
object Spark_Operator_SortByKey {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_SortByKey"))


    /**
     * sortByKey: 根据key进行排序，底层采用的是 RangPartitioner分区器
     * 传递两个参数： 第一个：正序true,倒序false
     * 第二个： 打乱重新分区以后，分区个数
     * sortBy底层默认采用的就是sortByKey
     */
    /*    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)
        val res: RDD[(String, Int)] = rdd.sortByKey(true, 3)
        res.saveAsTextFile("output")
        sc.stop()*/

    val rdd: RDD[(User, Int)] = sc.makeRDD(List((new User, 1), (new User, 2), (new User, 3), (new User, 4), (new User, 5)), 2)

    val res: RDD[(User, Int)] = rdd.sortByKey(true, 3)
    res.saveAsTextFile("output")
    sc.stop()


  }


}

case class User() extends Ordered[User] {
  override def compare(that: User): Int = -1
}
