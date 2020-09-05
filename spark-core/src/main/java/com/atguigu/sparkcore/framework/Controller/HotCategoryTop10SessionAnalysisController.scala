package com.atguigu.sparkcore.framework.Controller

import com.atguigu.sparkcore.framework.Service.{HotCategoryTop10SessionAnalysisService, QuestionOneService}
import org.apache.spark.rdd.RDD

/**
 * @ClassName: HotCategoryTop10SessionAnalysisController
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/10  22:47
 * @Version: 1.0
 */
class HotCategoryTop10SessionAnalysisController  extends Tcontroller {

  private val oneService: QuestionOneService = new QuestionOneService
  private val analysisService: HotCategoryTop10SessionAnalysisService = new HotCategoryTop10SessionAnalysisService

  override def dispatch(): Unit = {
    val top10: Array[(String, (Int, Int, Int))] = oneService.execute()

    val res: RDD[(Long, List[(String, Int)])] = analysisService.analysis(top10)
    res.foreach(println)


  }


}
