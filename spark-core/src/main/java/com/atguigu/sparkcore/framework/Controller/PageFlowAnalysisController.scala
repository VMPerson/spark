package com.atguigu.sparkcore.framework.Controller

import com.atguigu.sparkcore.framework.Service.PageFlowAnalysisService

/**
 * @ClassName: PageFlowAnalysisController
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/10  23:27
 * @Version: 1.0
 */
class PageFlowAnalysisController extends Tcontroller {

  private val pageFlowAnalysisService: PageFlowAnalysisService = new PageFlowAnalysisService


  override def dispatch(): Unit = {

    pageFlowAnalysisService.analysis()

  }
}
