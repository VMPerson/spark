package com.atguigu.sparkSql.day03

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @ClassName: Spark_Que_Query_One
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/14  10:35
 * @Version: 1.0
 */
object Spark_Que_Query_One {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Que_Query_One")

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    spark.sql("use spark")


    spark.sql(
      """
        |select  * from (
        |select
        |	*,rank() over(partition by area order by clickCount desc) as top
        |from
        |(select
        |   area,  product_name, count(*) as  clickCount
        |   from
        |(select a.*,
        |	   b.area,
        |    c.city_name,
        |	   c.product_name
        |from user_visit_action  a
        |join city_info  b on  a.city_id = b.city_id
        |join product_info c on a.click_product_id = c.product_id
        |  where a.click_product_id > -1
        |) t1
        | group by area,product_name
        | )t2
        | )t3 where top<=3
        |""".stripMargin).show()


    spark.stop()




  }





}
