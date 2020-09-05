package com.atguigu.sparkSql.day03

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @ClassName: Spark_Que_Data
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/14  9:37
 * @Version: 1.0
 */
object Spark_Que_Data {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Que_Data")

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    spark.sql("use spark")

    spark.sql(
      """
        |    CREATE TABLE `user_visit_action`(
        |  `date` string,
        |  `user_id` bigint,
        |  `session_id` string,
        |  `page_id` bigint,
        |  `action_time` string,
        |  `search_keyword` string,
        |  `click_category_id` bigint,
        |  `click_product_id` bigint,
        |  `order_category_ids` string,
        |  `order_product_ids` string,
        |  `pay_category_ids` string,
        |  `pay_product_ids` string,
        |  `city_id` bigint)
        | row format delimited fields terminated by '\t'
        |""".stripMargin)

    spark.sql(
      """
        |load data local inpath 'datas/user_visit_action.txt' into table spark.user_visit_action
        |""".stripMargin)
    spark.sql(
      """
        |CREATE TABLE `product_info`(
        |  `product_id` bigint,
        |  `product_name` string,
        |  `extend_info` string)
        |row format delimited fields terminated by '\t'
        |
        |""".stripMargin)
    spark.sql(
      """
        |load data local inpath 'datas/product_info.txt' into table spark.product_info
        |""".stripMargin)
    spark.sql(
      """
        |CREATE TABLE `city_info`(
        |  `city_id` bigint,
        |  `city_name` string,
        |  `area` string)
        |row format delimited fields terminated by '\t'
        |
        |""".stripMargin)
    spark.sql(
      """
        |load data local inpath 'datas/city_info.txt' into table spark.city_info
        |""".stripMargin)

    spark.stop()

  }


}
