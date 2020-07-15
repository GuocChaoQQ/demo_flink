package sink

import java.sql.Timestamp

import com.alibaba.fastjson.JSON
import domain.{BaseAd, BaseViplevel, BaseWebSite, GlobalConfig, TopicAndValue}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.kudu.client.{KuduClient, KuduSession, KuduTable}

/**
 * created by chao.guo on 2020/7/15
 **/
class DimKuduSink extends  RichSinkFunction[TopicAndValue]{
  var kuduClient: KuduClient = _
  var kuduSession: KuduSession = _
  var test_adTable: KuduTable = _
  var test_WebSiteTable: KuduTable = _
  var test_VipLevelTable: KuduTable = _

//** 处理广告

  // 建表语句





  def invokeAd(value: TopicAndValue): Unit = {
    val baseAd: BaseAd = JSON.parseObject(value.value,classOf[BaseAd])
    val upsert = test_adTable.newUpsert()
    val row = upsert.getRow()
    row.addInt("adid", baseAd.adid)
    row.addString("adname", baseAd.adname)
    row.addString("dn", baseAd.dn)
    kuduSession.apply(upsert)
  }

  /**
   * 处理网站数据
   * @param value
   */
  def invokeWeb(value: TopicAndValue): Unit = {
    val baseWeb: BaseWebSite = JSON.parseObject(value.value,classOf[BaseWebSite])
    val upsert = test_WebSiteTable.newUpsert()
    val row = upsert.getRow
    row.addInt("siteid", baseWeb.siteid)
    row.addString("sitename", baseWeb.sitename)
    row.addString("siteurl", baseWeb.siteurl)
    row.addString("delete", baseWeb.delete)
    row.addString("createtime", baseWeb.createtime)
    row.addString("creator", baseWeb.creator)
    row.addString("dn", baseWeb.dn)
    kuduSession.apply(upsert)

  }

  /**
   * 处理vip 数据
   * @param value
   */
  def invokeVip(value: TopicAndValue): Unit = {
    val vip: BaseViplevel = JSON.parseObject(value.value,classOf[BaseViplevel])
    val upsert = test_VipLevelTable.newUpsert()
    val row = upsert.getRow
    row.addInt("vip_id", vip.vip_id)
    row.addString("vip_level", vip.vip_level)
    row.addTimestamp("start_time", Timestamp.valueOf(vip.start_time))
    row.addTimestamp("end_time", Timestamp.valueOf(vip.end_time))
    row.addTimestamp("last_modify_time", Timestamp.valueOf(vip.last_modify_time))
    row.addString("max_free", vip.max_free)
    row.addString("min_free", vip.min_free)
    row.addString("next_level", vip.next_level)
    row.addString("operator", vip.operator)
    row.addString("dn", vip.dn)
    kuduSession.apply(upsert)


  }

  override def invoke(value: TopicAndValue, context: SinkFunction.Context[_]): Unit = {
    value.topic match {
      case "test_ad" =>invokeAd(value)
      case "test_web"=>invokeWeb(value)
      case "test_vip" =>invokeVip(value)
    }

  }

  override def open(parameters: Configuration): Unit = {
    kuduClient = new KuduClient.KuduClientBuilder(GlobalConfig.KUDU_MASTER).build()
    kuduSession = kuduClient.newSession()
    kuduSession.setTimeoutMillis(60000)
    test_adTable = kuduClient.openTable(GlobalConfig.KUDU_TABLE_DWDBASEAD)
    test_WebSiteTable = kuduClient.openTable(GlobalConfig.KUDU_TABLE_DWDBASEWEBSITE)
    test_VipLevelTable = kuduClient.openTable(GlobalConfig.KUDU_TABLE_DWDVIPLEVEL)


  }

  override def close(): Unit = {
    kuduSession.close()
    kuduClient.close()

  }
}
