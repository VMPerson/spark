package com.atguigu.sparkcore.framework.Common

import com.atguigu.sparkcore.framework.Utils.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: TConfigrution
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/10  9:40
 * @Version: 1.0
 */
trait TApplication {


  def runApplication(master: String = "local[*]", appName: String = "TConfigrution")(op: => Unit): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setMaster(master).setAppName(appName))
    EnvUtil.put(sc)

    try {
      op
    } catch {
      case ex: Exception => throw ex
    }

    sc.stop()

  }


}
