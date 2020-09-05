package com.atguigu.sparkcore.framework.Service

import com.atguigu.sparkcore.framework.Dao.PageFlowAnalysisDao
import com.atguigu.sparkcore.framework.bean.UserVisitAction
import org.apache.spark.rdd.RDD

/**
 * @ClassName: PageFlowAnalysisService
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/10  23:28
 * @Version: 1.0
 */
class PageFlowAnalysisService extends TService {

  private val pageFlowAnalysisDao: PageFlowAnalysisDao = new PageFlowAnalysisDao

  override def analysis(): Any = {

    val fileRDD: RDD[String] = pageFlowAnalysisDao.readFile("datas/user_visit_action.txt")

    val userVisitAction = fileRDD.map(
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
    userVisitAction.cache()
    val list = List(1, 2, 3, 4, 5, 6, 7)
    val okPageidZipList = list.zip(list.tail)
    val filterRdd: RDD[UserVisitAction] = userVisitAction.filter(elem => {
      list.init.contains(
        elem.page_id.toInt
      )
    })

    //计算分母
    val rdd1: RDD[(Long, Int)] = filterRdd.map(elem => {
      (elem.page_id, 1)
    })
    val fenmu: Map[Long, Int] = rdd1.reduceByKey(_ + _).collect().toMap


    //分子计算
    val rdd2: RDD[(String, Iterable[UserVisitAction])] = userVisitAction.groupBy(_.session_id)
    val rdd5: RDD[(String, List[((Long, Long), Int)])] = rdd2.mapValues(elem => {
      //根据时间进行排序
      val actions: List[UserVisitAction] = elem.toList.sortWith((x, y) => {
        x.action_time < y.action_time
      })
      val pageIds: List[Long] = actions.map(_.page_id)

      val zipData: List[(Long, Long)] = pageIds.zip(pageIds.tail)

      zipData.map {
        case (k1, k2) => {
          ((k1, k2), 1)
        }
      }.filter {
        case (k, _) => {
          okPageidZipList.contains(k)
        }
      }

    })
    val rdd6: RDD[List[((Long, Long), Int)]] = rdd5.map(_._2)
    val rdd7: RDD[((Long, Long), Int)] = rdd6.flatMap(list => list)
    val rdd8: RDD[((Long, Long), Int)] = rdd7.reduceByKey(_ + _)
    rdd8.foreach {
      case ((id1, id2), sum) => {
        // 根据ID1查找对应的分母的数据
        val total = fenmu.getOrElse(id1, 1)
        println(s"页面跳转【${id1}-${id2}】的转换率为:" + (sum.toDouble / total))
      }

    }
  }


}
