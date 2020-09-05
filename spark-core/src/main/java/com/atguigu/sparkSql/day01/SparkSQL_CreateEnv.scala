package com.atguigu.sparkSql.day01

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @ClassName: CreateEnv
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/11  20:51
 * @Version: 1.0
 */
object SparkSQL_CreateEnv {

  def main(args: Array[String]): Unit = {

    /*  SQLSession对象构造器私有化了，不能通过new的方式创建
      scala中构造方法存在两种类型
            1.主构造方法：在类名后声明够着方法被称为主够着方法
                在参数列表前，加private关键字，表示够着方法私有化
            2.辅助构造方法：使用this关键字声明的构造方法
                在辅助方法前加private[包名]  ：包访问权限
      如果一个类对外没有提供构造方法，表示不希望在外部创建对象
      一般会在内部创建对象，然后通过静态的方法返回这个对象，即scala的伴生对象

      */
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("createEnv")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()


    spark.stop()

  }


}
