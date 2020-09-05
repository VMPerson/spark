package com.atguigu.sparkcore.framework.Application

import com.atguigu.sparkcore.framework.Common.TApplication
import com.atguigu.sparkcore.framework.Controller.PageFlowAnalysisController

/**
 * @ClassName: PageFlowAnalysisApplication
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/10  23:27
 * @Version: 1.0
 */
object PageFlowAnalysisApplication extends App with TApplication {

  runApplication(){
    val controller: PageFlowAnalysisController = new PageFlowAnalysisController
    controller.dispatch()
  }




}
