package com.atguigu.sparkSql.day02

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @ClassName: Spark_Connnect_Nei
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/13  23:50
 * @Version: 1.0
 */
object Spark_Connnect_Nei {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Connnect_Nei")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    import spark.implicits._
    spark.sql("create table abc(id int)")
    spark.sql("load data local inpath 'datas/ids.txt' into table abc")
    Thread.sleep(5000)
    spark.sql("show tables").show()
    Thread.sleep(5000)
    spark.sql("select * from abc ").show()


    spark.stop()


  }


}
