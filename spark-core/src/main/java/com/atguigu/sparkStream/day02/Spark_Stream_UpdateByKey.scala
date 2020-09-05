package com.atguigu.sparkStream.day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName: Spark_Stream_UpdateByKey
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/16  19:42
 * @Version: 1.0
 */
object Spark_Stream_UpdateByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Stream_UpdateByKey")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    ssc.checkpoint("cp")
    val inputStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9988)

    //无状态转化  所谓的无状态转化 ，简单理解就是每个采集周期之间或不影响(不保留中间阶段结果)
    //   val ds: DStream[(String, Int)] = inputStream.map((_, 1)).reduceByKey(_ + _)
    // ds.print()

    //有状态计算  有状态操作的缓冲区一般都是存储在检查点中的
    val resDs: DStream[(String, Int)] = inputStream.map((_, 1)).updateStateByKey((valueSeq: Seq[Int], buffer: Option[Int]) => {
      //类似于reduce
      val newCount: Int = buffer.getOrElse(0) + valueSeq.sum
      //要求返回一个Option
      Option(newCount)
    })
    resDs.print()


    ssc.start()
    ssc.awaitTermination()

  }


}
