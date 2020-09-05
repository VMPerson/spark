package com.atguigu.sparkStream.day02

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * @ClassName: Spark_Stream_MockData
 * @Description: TODO 生成时时的模拟数据
 * @Author: VmPerson
 * @Date: 2020/8/16  22:40
 * @Version: 1.0
 */
object Spark_Stream_MockData {


  def main(args: Array[String]): Unit = {


    val properties: Properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    //向Kafka中发送数据
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)
    while (true) {
      for (data <- generateMockData()) {
        producer.send(new ProducerRecord[String, String]("SparkDemo", data))
      }

      Thread.sleep(2000)
    }


  }

  //模拟生成数据
  def generateMockData() = {
    val list = ListBuffer[String]()
    var areaList = List("华南", "华北", "华中")
    var cityList = List("北京", "郑州", "南昌", "深圳")


    for (i <- 1 to new Random().nextInt(10)) {
      //某个时间点 某个地区 某个城市 某个用户 某个广告

      var area = areaList(new Random().nextInt(3))
      var city = cityList(new Random().nextInt(4))
      var userId = new Random().nextInt(6) + 1
      var adId = new Random().nextInt(6) + 1
      list.append(s"${System.currentTimeMillis()} ${area} ${city} ${userId} ${adId}")
    }
    list
  }


}
