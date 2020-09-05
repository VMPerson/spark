package com.atguigu.sparkcore.framework.Utils

import org.apache.spark.SparkContext

/**
 * @ClassName: EnvUtil
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/10  22:07
 * @Version: 1.0
 */
object EnvUtil {


  private val scThreadCache: ThreadLocal[SparkContext] = new ThreadLocal[SparkContext]()

  def put(sc: SparkContext): Unit = {
    scThreadCache.set(sc)
  }

  def get() = {
    scThreadCache.get()
  }

  def clear(): Unit = {
    scThreadCache.remove()
  }


}
