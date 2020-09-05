package com.atguigu.sparkStream.day02

import java.sql.ResultSet
import java.text.SimpleDateFormat

import com.atguigu.sparkStream.day02.Spark_Stream_Question_One_One.AdClickData
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
 * @ClassName: Spark_Stream_Question_One_Two
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/16  22:22
 * @Version: 1.0
 */
object Spark_Stream_Question_One_Two {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Stream_Question_One_Two")

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

        val conn = JDBCUtil.getConnection
        val pstat = conn.prepareStatement("select userid from black_list")
        val rs: ResultSet = pstat.executeQuery()
        // 黑名单数据
        val blackList = ListBuffer[String]()
        while (rs.next()) {
          blackList.append(rs.getString(1))
        }
        rs.close()
        pstat.close()
        conn.close()

        //对黑名单数据进行过滤
        val filterRDD: RDD[AdClickData] = rdd.filter(data => {
          !blackList.contains(data.userId)
        })

        filterRDD.map(
          elem => {
            val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
            val day: String = sdf.format(new java.util.Date(elem.ts.toLong))
            ((day, elem.userId, elem.adId), 1)
          }
        ).reduceByKey(_ + _)
      }
    )

    //更新mysql中的数据 将当前数据和历史数据进行合并
    // TODO 更新Mysql中用户广告点击数据
    // 将当前的数据和今天历史数据进行合并
    adClickSumDS.foreachRDD(
      rdd => {
        //（（天，用户，广告），sum）
        rdd.foreach{
          case ( (day, userid, adid), sum ) => {
            val conn = JDBCUtil.getConnection

            // 更新用户点击统计表
            val insertPstat = conn.prepareStatement(
              """
                |insert into user_ad_count(dt, userid, adid, count)
                |values (?, ?, ?, ?)
                |on duplicate key
                |update count = count + ?
                            """.stripMargin)

            // 判断是否超过阈值
            val selectPstat = conn.prepareStatement(
              """
                |select
                |    userid
                |from user_ad_count
                |where dt = ? and userid = ? and adid = ? and count >= 20
                            """.stripMargin)

            // 加入黑名单
            val blackListPstat = conn.prepareStatement(
              """
                |insert into black_list (userid) values (?)
                |on duplicate key
                |update userid = ?
                            """.stripMargin)


            // TODO 更新统计表的数据
            insertPstat.setString(1, day)
            insertPstat.setString(2, userid)
            insertPstat.setString(3, adid)
            insertPstat.setInt(4, sum)
            insertPstat.setInt(5, sum)
            insertPstat.executeUpdate()

            // TODO 查询超过阈值的数据
            selectPstat.setString(1, day)
            selectPstat.setString(2, userid)
            selectPstat.setString(3, adid)
            val rs = selectPstat.executeQuery()

            if ( rs.next() ) {
              // TODO 插入黑名单
              blackListPstat.setString(1, userid)
              blackListPstat.setString(2, userid)
              blackListPstat.executeUpdate()
            }

            rs.close()
            insertPstat.close()
            selectPstat.close()
            blackListPstat.close()

            conn.close()
          }
        }
      }
    )

    ssc.start()
    ssc.awaitTermination()

  }














}
