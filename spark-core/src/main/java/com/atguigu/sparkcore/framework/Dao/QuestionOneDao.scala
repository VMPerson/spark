package com.atguigu.sparkcore.framework.Dao

import com.atguigu.sparkcore.framework.Utils.EnvUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @ClassName: WorldCountDao
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/10  9:58
 * @Version: 1.0
 */
class QuestionOneDao {
  def readFile(str: String) = {
    EnvUtil.get().textFile(str)
  }

}
