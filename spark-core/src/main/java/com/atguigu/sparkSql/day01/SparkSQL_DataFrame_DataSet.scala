package com.atguigu.sparkSql.day01

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: SparkSQL_RDD_DataFrame
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/11  21:06
 * @Version: 1.0
 */
object SparkSQL_DataFrame_DataSet {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_DataFrame_DataSet")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    val df: DataFrame = spark.read.json("datas/user.json")


    //df转换为ds
    val ds: Dataset[User] = df.as[User]
    ds.show()

    //ds转df
    val resDf: DataFrame = ds.toDF("username", "count")
    resDf.show()


    spark.stop()

  }


}

case class User(name: String, age: Long)