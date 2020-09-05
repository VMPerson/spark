package com.atguigu.sparkcore.framework.Service

import com.atguigu.sparkcore.framework.Dao.HotCategoryTop10SessionAnalysisDao
import com.atguigu.sparkcore.framework.Utils.EnvUtil
import com.atguigu.sparkcore.framework.bean.UserVisitAction
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
 * @ClassName: HotCategoryTop10SessionAnalysisService
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/10  22:48
 * @Version: 1.0
 */
class HotCategoryTop10SessionAnalysisService extends TService {

  private val analysisDao: HotCategoryTop10SessionAnalysisDao = new HotCategoryTop10SessionAnalysisDao

  override def analysis(data: Any)= {

    val fileRDD: RDD[String] = analysisDao.readFile("datas/user_visit_action.txt")

    val top10Datas: Array[(String, (Int, Int, Int))] = data.asInstanceOf[Array[(String, (Int, Int, Int))]]
    val top10Ids: Array[String] = top10Datas.map(_._1)
    val ids: Broadcast[Array[String]] = EnvUtil.get().broadcast(top10Ids)

    val userVisitAction: RDD[UserVisitAction] = fileRDD.map(
      action => {
        val datas = action.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    )
    val rdd1: RDD[UserVisitAction] = userVisitAction.filter(
      elem => {
        ids.value.contains(elem.click_category_id.toString)
      })


    val rdd2: RDD[((Long, String), Int)] = rdd1.map(elem => {
      ((elem.click_category_id, elem.session_id), 1)
    })
    val rdd3: RDD[((Long, String), Int)] = rdd2.reduceByKey(_ + _)

    val rdd4: RDD[(Long, (String, Int))] = rdd3.map {
      case ((id, session), sum) => {
        (id, (session, sum))
      }
    }
    val rdd5: RDD[(Long, Iterable[(String, Int)])] = rdd4.groupByKey()
    val res: RDD[(Long, List[(String, Int)])] = rdd5.map {
      elem => {
        (elem._1, elem._2.toList.sortWith((x, y) => {
          x._2 > y._2
        }).take(10))
      }
    }
    res
  }

}

