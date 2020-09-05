package com.atguigu.sparkStream.day02

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
 * @ClassName: Spark_Stream_Question_One
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/16  22:22
 * @Version: 1.0
 */
object Spark_Stream_Question_One_One {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Stream_Question_One_One")

    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    val kafkaMap = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    )

    val kaFkaData: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("SparkDemo"), kafkaMap)
    )
    //获取用户广告点击的数据
    val adClickData: DStream[AdClickData] = kaFkaData.map(
      elem => {
        val tmp: Array[String] = elem.value().split(" ")
        AdClickData(tmp(0), tmp(1), tmp(2), tmp(3), tmp(4))
      })


    //（周期性获取）获取黑名单的数据
    val adClickSumDS: DStream[((String, String, String), Int)] = adClickData.transform(
      rdd => {
        //读取mysql黑名单的数据

        val driverClass = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://hadoop102:3306/Spark"
        val username = "root"
        val password = "123456"
        Class.forName(driverClass)
        val conn: Connection = DriverManager.getConnection(url, username, password)
        var hSql = "select userid  from black_list"
        val preparedStatement: PreparedStatement = conn.prepareStatement(hSql)
        val hResSet: ResultSet = preparedStatement.executeQuery()
        var blackList = ListBuffer[String]()
        while (hResSet.next()) {
          blackList.append(hResSet.getString(1))
        }
        hResSet.close()
        preparedStatement.close()
        conn.close()

        //对黑名单数据进行过滤
        val filterRDD: RDD[AdClickData] = rdd.filter(data => {
          !blackList.contains(data.userId)
        })

        filterRDD.map(
          elem => {
            val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
            val day: String = sdf.format( new java.util.Date(elem.ts.toLong))
            ((day, elem.userId, elem.adId), 1)
          }
        ).reduceByKey(_ + _)
      }
    )

    //更新mysql中的数据 将当前数据和历史数据进行合并
    adClickSumDS.foreachRDD(
      rdd => {
        rdd.foreach {
          case ((day, userId, adId), sum) => {
            var driverClass = "com.mysql.jdbc.Driver"
            val url = "jdbc:mysql://hadoop102:3306/Spark"
            var username = "root"
            var password = "123456"
            Class.forName(driverClass)
            val conn: Connection = DriverManager.getConnection(url, username, password)
            //本批次如果点击次数超过阈值，直接加入黑名单
            if (sum > 10) {
              var iSql =
                """
                  |insert into black_list (userid) values ?
                  |on duplicate key
                  |update userid=?
                  |""".stripMargin
              val statement: PreparedStatement = conn.prepareStatement(iSql)
              statement.setString(1, userId)
              statement.setString(2, userId)
              statement.executeUpdate()
              statement.close()
            } else {
              //如果没有大于阈值 ，从数据库里面读取出今天的次数和这次的相加，如果大于阈值，则加入黑名单，如果没有大于阈值则更新点击次数表
              var qSql =
                """
                  |select
                  |    count
                  |from user_ad_count
                  |where dt = ? and userid = ? and adid = ?
                  |""".stripMargin
              val sStatement: PreparedStatement = conn.prepareStatement(qSql)
              sStatement.setString(1, day)
              sStatement.setString(2, userId)
              sStatement.setString(3, adId)
              val clickCount: ResultSet = sStatement.executeQuery()
              if (clickCount.next()) {
                val oldCount: Int = clickCount.getInt(1)
                var newCount = oldCount + sum
                if (newCount > 10) {
                  var iSql =
                    """
                      |insert into black_list (userid) values (?)
                      |on duplicate key
                      |update userid=?
                      |""".stripMargin
                  val statement: PreparedStatement = conn.prepareStatement(iSql)
                  statement.setString(1, userId)
                  statement.setString(2, userId)
                  statement.executeUpdate()
                  statement.close()
                } else {
                  var uSql =
                    """
                      |update user_ad_count
                      |set count=?
                      |where  dt=? and  userid=? and adid=?
                      |""".stripMargin

                  val updateStatement: PreparedStatement = conn.prepareStatement(uSql)
                  updateStatement.setInt(1, newCount)
                  updateStatement.setString(2, day)
                  updateStatement.setString(3, userId)
                  updateStatement.setString(4, adId)
                  updateStatement.executeUpdate()
                  updateStatement.close()
                }
              } else {
                //数据不存在的场合，之前就没有点击数据直接插入点击次数表
                val insertPstat = conn.prepareStatement(
                  """
                    |insert into user_ad_count(dt, userid, adid, count) values (?, ?, ?, ?)
                                    """.stripMargin)
                insertPstat.setString(1, day)
                insertPstat.setString(2, userId)
                insertPstat.setString(3, adId)
                insertPstat.setInt(4, sum)
                insertPstat.executeUpdate()
                insertPstat.close()
              }

              clickCount.close()
              sStatement.close()
              conn.close()
            }
          }
        }
      }
    )

    ssc.start()
    ssc.awaitTermination()

  }


  case class AdClickData(ts: String, area: String, city: String, userId: String, adId: String)


}
