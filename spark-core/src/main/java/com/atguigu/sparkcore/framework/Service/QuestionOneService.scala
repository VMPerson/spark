package com.atguigu.sparkcore.framework.Service

import com.atguigu.sparkcore.framework.Dao.QuestionOneDao
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @ClassName: WorldCountService
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/10  9:57
 * @Version: 1.0
 */
class QuestionOneService {
  private val dao: QuestionOneDao = new QuestionOneDao

  def execute() = {
    val fileRdd: RDD[String] = dao.readFile( "datas/user_visit_action.txt")

    val rdd: RDD[(String, (Int, Int, Int))] = fileRdd.flatMap(
      elem => {
        var tmp: Array[String] = elem.split("_")
        if (tmp(6) != "-1") {
          List((tmp(6), (1, 0, 0)))
        } else if (tmp(8) != "null") {
          val ids: Array[String] = tmp(8).split(",")
          ids.map((_, (0, 1, 0)))
        } else if (tmp(10) != "null") {
          val ids: Array[String] = tmp(10).split(",")
          ids.map((_, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    val mmp: RDD[(String, (Int, Int, Int))] = rdd.reduceByKey {
      case ((x1, x2, x3), (y1, y2, y3)) => {
        (x1 + y1, x2 + y2, x3 + y3)
      }
    }

    mmp.sortBy(_._2, false).take(10)


  }

}
