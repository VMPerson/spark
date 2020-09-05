package com.atguigu.sparkcore.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Operator_Map {


  //体验map算子
  def main(args: Array[String]): Unit = {

   // val sparkConf: SparkConf =
    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_Map"))

    val listRdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //map方法会将数据源中的每一条数据进行map的转换操作，每一条数据都会经过转化的方法
    //map方法返回的数据类型并没有确定，可以根据业务需求去确定，所以扩展性很强，也是算子里面使用最为频繁的算子
    //每一次使用,都需要传函数，很麻烦，所以我们使用匿名行数的方式解决这一问题

    //将集合里的数据都乘以2
    //val res: RDD[Int] = listRdd.map((i:Int)=>{i*2})
    //val res: RDD[Int] = listRdd.map((i:Int)=>i*2)
    //val res: RDD[Int] = listRdd.map((i)=>i*2)
    //val res: RDD[Int] = listRdd.map(i=>i*2)
    val res: RDD[Int] = listRdd.map((_ * 2))

    res.collect foreach println


  }


}
