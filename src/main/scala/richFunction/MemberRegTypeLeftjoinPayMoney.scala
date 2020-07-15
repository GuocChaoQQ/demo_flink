package richFunction

import java.lang

import domain.DwdMemberPayMoney
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.util.Collector
import utils.ParseJsonData

/**
 * created by chao.guo on 2020/7/15
 **/
class MemberRegTypeLeftjoinPayMoney  extends CoGroupFunction[String,DwdMemberPayMoney,String]{
  override def coGroup(left: lang.Iterable[String], right: lang.Iterable[DwdMemberPayMoney], collector: Collector[String]): Unit = {
    var bl = false
    val leftIterator = left.iterator()
    val rightIterator = right.iterator()
    while (leftIterator.hasNext) {
      val jsonObject = ParseJsonData.getJsonData(leftIterator.next())
      while (rightIterator.hasNext) {
        val dwdMemberPayMoney = rightIterator.next()
        jsonObject.put("paymoney", dwdMemberPayMoney.paymoney)
        jsonObject.put("siteid", dwdMemberPayMoney.siteid)
        jsonObject.put("vip_id", dwdMemberPayMoney.vip_id)
        bl = true
      }
      if (!bl) {
        jsonObject.put("paymoney", "")
        jsonObject.put("siteid", "")
        jsonObject.put("vip_id", "")
      }
      collector.collect(jsonObject.toJSONString)
    }
  }
}
