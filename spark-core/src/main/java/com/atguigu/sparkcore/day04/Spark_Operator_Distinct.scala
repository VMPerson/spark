package com.atguigu.sparkcore.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Operator_Distinct {


  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_Distinct"))

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 1, 2, 2, 3, 3, 6), 2)

    //Scala中 distinct方法去重的实现逻辑是 通过HashSet来实现的
    //在spark中 distinct主要实现逻辑
    //map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
    // 相当于做了一个映射，数据源的每个值作为key,value为null,然后通过reduceByKey对相同的key做处理，
    // 只保留key,value不关注，在做一个map映射，只取key，来实现去重效果

    val res: RDD[Int] = rdd.distinct()

    res.collect().foreach(println)

  }


}
