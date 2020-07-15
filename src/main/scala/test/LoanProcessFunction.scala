package test

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import test.ClassCase.Loan

/**
 * created by chao.guo on 2020/4/24
 **/
class LoanProcessFunction(val map: Map[String,OutputTag[Loan]]) extends KeyedProcessFunction[String,Loan,Loan]{
var flagStatus:ValueState[Boolean] =_
  var timerState :ValueState[Long]=_
  var min_amount:ValueState[BigDecimal]=_

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Loan, Loan]#OnTimerContext, out: Collector[Loan]): Unit = {

    timerState.clear()
    flagStatus.clear()
  }

  override def open(parameters: Configuration): Unit = {
    //同于保存标志位
    val flagDescriptor = new ValueStateDescriptor("flagStatus",classOf[Boolean])
    flagStatus = getRuntimeContext.getState(flagDescriptor)
    //  用于存储定时任务的时间
    val timerDescriptor = new ValueStateDescriptor("timerState", classOf[Long])
    timerState = getRuntimeContext.getState(timerDescriptor)

    val min_aount = new ValueStateDescriptor("min_amount",classOf[BigDecimal])
    min_amount = getRuntimeContext.getState(min_aount)

  }

  override def processElement(i: Loan, context: KeyedProcessFunction[String, Loan, Loan]#Context, collector: Collector[Loan]): Unit = {
    val flag = flagStatus.value()
    val decimal = min_amount.value()
//    println(i)
    //如果存在状态值
      val loan_amount = i.loan_amount
      if(decimal!=null){
        if(loan_amount<decimal){ // 不等于空 比较当前账户的最小值金额 更新状态
          min_amount.update(loan_amount)
        }
        if(loan_amount>=0 && loan_amount<=100){
          context.output(map("stream_0_100"),i)
        }else if (loan_amount>100 && loan_amount <=200){
          context.output(map("stream_100_200"),i)

        }else {
          context.output(map("stream_200"),i)
        }
//        if((loan_amount - decimal) >=300 ||(decimal -loan_amount  ) >=300 ){ // 最大值和最小值相差两百
        //////          collector.collect(i) // 输出两百
        ////
        ////
        ////
        ////        }
      }else{ // 等于空 则
        min_amount.update(loan_amount) // 注册定时任务
        val timer = context.timerService.currentProcessingTime + 60*10000

        context.timerService.registerProcessingTimeTimer(timer)
        timerState.update(timer)
      }


  }

  private def cleanUp(ctx: KeyedProcessFunction[Long, Loan, Loan]#Context): Unit = {
    // delete timer
    val timer = timerState.value
    ctx.timerService.deleteProcessingTimeTimer(timer)

    // clean up all states
    timerState.clear()
    flagStatus.clear()
  }

}
