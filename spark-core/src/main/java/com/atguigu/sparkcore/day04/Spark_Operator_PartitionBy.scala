package com.atguigu.sparkcore.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


object Spark_Operator_PartitionBy {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_PartitionBy"))

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    val mapRdd: RDD[(Int, Null)] = rdd.map((_, null))

    //partitionBy算子 ： 表示根据指定规则进行重分区
    //    使用时需要注意以下几点：
    //      1.partitionBy算子是针对(key,value)数据集进行分区的，因为底层他是用到了key，所以如果是单列数据集的话，
    //       先进行一下map映射再进行重新分区，完成后再进行map映射取到原值
    //    Spark给我们提供了两种分区器： HashPartitioner、RangePartitioner
    //    Spark默认使用的是HashPartitioner  ： HashPartitoner底层以来的是模运算进行计算的
    //     为什么他底层采用模运算而不采用位运算？
    //        因为采用位运算是有前提条件的，当是使用位运算时，需要分区长度必须为2的幂次方，不然会有数据分区分不到数据，而出现数据倾斜
    //Spark中的KV对象方法一般都声明在 PairRDDFunctions里面

    val tmp: RDD[(Int, Null)] = mapRdd.partitionBy(new HashPartitioner(3))
    val res: RDD[Int] = tmp.map(_._1)
    res.saveAsTextFile("output4")
    sc.stop()


  }

}
