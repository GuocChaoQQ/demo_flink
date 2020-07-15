package richFunction

import java.util
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.google.common.cache.{Cache, CacheBuilder}
import domain.GlobalConfig
import org.apache.commons.lang3.StringUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.util.ExecutorUtils
import org.apache.kudu.client.KuduPredicate.ComparisonOp
import org.apache.kudu.client.SessionConfiguration.FlushMode
import org.apache.kudu.client.{KuduClient, KuduPredicate, KuduSession, KuduTable}
import utils.ParseJsonData

/**
 * created by chao.guo on 2020/7/15
 **/
class DimKuduAsyncFunction extends  RichAsyncFunction[String, String]{
  var executorService: ExecutorService = _
  var cache: Cache[String, String] = _
  var kuduClient: KuduClient = _
  var kuduSession: KuduSession = _
  var dwbBaseadTable: KuduTable = _
  var dwbBaseWebSiteTable: KuduTable = _
  var dwbVipLevelTable: KuduTable = _

  override def open(parameters: Configuration): Unit = {
    kuduClient = new KuduClient.KuduClientBuilder(GlobalConfig.KUDU_MASTER).build()
    kuduSession = kuduClient.newSession()
    kuduSession.setFlushMode(FlushMode.AUTO_FLUSH_SYNC)
    dwbBaseadTable = kuduClient.openTable(GlobalConfig.KUDU_TABLE_DWDBASEAD)
    dwbBaseWebSiteTable = kuduClient.openTable(GlobalConfig.KUDU_TABLE_DWDBASEWEBSITE)
    dwbVipLevelTable = kuduClient.openTable(GlobalConfig.KUDU_TABLE_DWDVIPLEVEL)
    executorService = Executors.newFixedThreadPool(6)
    cache = CacheBuilder.newBuilder()
      .concurrencyLevel(6) //设置并发级别 允许6个线程同时访问
      .expireAfterAccess(2, TimeUnit.HOURS) //设置过期时间
      .maximumSize(10000) //设置缓存大小
      .build()
  }

  override def close(): Unit = {
    kuduSession.close()
    kuduClient.close()
    ExecutorUtils.gracefulShutdown(100, TimeUnit.MILLISECONDS, executorService);
  }

  override def asyncInvoke(input: String, resultFuture: ResultFuture[String]): Unit = {
    executorService.submit(new Runnable {
      override def run(): Unit = {
        // 查询kudu 表中对应的广告信息表
        var jSONObject = getBaseAd(input)
        // 查询kudu 表对应的网站信息
         jSONObject = getBaseWebSite(jSONObject)
        //查询kudu 表对应的 vip信息表
        jSONObject = getBaseVip(jSONObject)
        resultFuture.complete(Array(jSONObject.toJSONString))
      }
    })


  }

  /**
   * 查询vip 信息
   * @param jSONObject
   * @return
   */
  def getBaseVip(jSONObject: JSONObject): JSONObject = {
    val vip_id = jSONObject.getInteger("vip_id")
    val dn = jSONObject.getString("dn")
    var vip_level: String = ""
    var vip_start_time: String = ""
    var vip_end_tiem: String = ""
    var last_modify_time = ""
    var max_free = ""
    var min_free = ""
    var next_level = ""
    var operator = ""
    //查询vip表关联数据
    if (null != vip_id) {
      val vipDetail = cache.getIfPresent("vipDetail:" + vip_id + "_" + dn)
      if (vipDetail == null || "".equals(vipDetail)) {
        val schma = dwbVipLevelTable.getSchema
        //声明查询条件  vip_id相等
        val eqvipidPred = KuduPredicate.newComparisonPredicate(schma.getColumn("vip_id"), ComparisonOp.EQUAL, vip_id)
        //声明查询条件 dn相等
        val eqdnPred = KuduPredicate.newComparisonPredicate(schma.getColumn("dn"), ComparisonOp.EQUAL, dn)
        //声明查询字段
        val list = new util.ArrayList[String]()
        list.add("vip_level")
        list.add("start_time")
        list.add("end_time")
        list.add("last_modify_time")
        list.add("max_free")
        list.add("min_free")
        list.add("next_level")
        list.add("operator")
        //查询
        val kuduScanner = kuduClient.newScannerBuilder(dwbVipLevelTable).setProjectedColumnNames(list).addPredicate(eqvipidPred)
          .addPredicate(eqdnPred).build()
        while (kuduScanner.hasMoreRows) {
          val results = kuduScanner.nextRows()
          while (results.hasNext) {
            val result = results.next()
            vip_level = result.getString("vip_level")
            vip_start_time = result.getTimestamp("start_time").toString
            vip_end_tiem = result.getTimestamp("end_time").toString
            last_modify_time = result.getTimestamp("last_modify_time").toString
            max_free = result.getString("max_free")
            min_free = result.getString("min_free")
            next_level = result.getString("next_level")
            operator = result.getString("operator")
          }
        }
        //将查询到的数据拼装成json 存入缓存
        val jsonObject = new JSONObject()
        jsonObject.put("vip_level", vip_level)
        jsonObject.put("vip_start_time", vip_start_time)
        jsonObject.put("vip_end_tiem", vip_end_tiem)
        jsonObject.put("last_modify_time", last_modify_time)
        jsonObject.put("max_free", max_free)
        jsonObject.put("min_free", min_free)
        jsonObject.put("next_level", next_level)
        jsonObject.put("operator", operator)
        cache.put("vipDetail:" + vip_id + "_" + dn, jsonObject.toJSONString)
      } else {
        //如果缓存中有值 就解析缓存中的数据
        val jsonObject = ParseJsonData.getJsonData(vipDetail)
        vip_level = jsonObject.getString("vip_level")
        vip_start_time = jsonObject.getString("vip_start_time")
        vip_end_tiem = jsonObject.getString("vip_end_tiem")
        last_modify_time = jsonObject.getString("last_modify_time")
        max_free = jsonObject.getString("max_free")
        min_free = jsonObject.getString("min_free")
        next_level = jsonObject.getString("next_level")
        operator = jsonObject.getString("operator")
      }
    }
    jSONObject.put("vip_level", vip_level)
    jSONObject.put("vip_start_time", vip_start_time)
    jSONObject.put("vip_end_time", vip_end_tiem)
    jSONObject.put("last_modify_time", last_modify_time)
    jSONObject.put("max_free", max_free)
    jSONObject.put("min_free", min_free)
    jSONObject.put("next_level", next_level)
    jSONObject.put("operator", operator)
    jSONObject

  }

//查询网站信息
  def getBaseWebSite(jSONObject: JSONObject): JSONObject = {
    val siteid = jSONObject.getInteger("siteid")
    val dn = jSONObject.getString("dn")
    var sitename: String = ""
    var siteurl: String = ""
    var delete: String = ""
    var site_createtime: String = ""
    var site_creator: String = ""
    //查询网站关联表 sitename siteurl等信息
    if (null != siteid) {
      //先从缓存取数据
      val siteDetail = cache.getIfPresent("siteDetail:" + siteid + "_" + dn)
      if (siteDetail == null || "".equals(siteDetail)) {
        //查询kudu

        val schema = dwbBaseWebSiteTable.getSchema
        //声明查询条件  site_id相等
        val eqsiteidPred = KuduPredicate.newComparisonPredicate(schema.getColumn("siteid"), ComparisonOp.EQUAL, siteid)
        //声明查询条件  dn相等
        val eqdnPred = KuduPredicate.newComparisonPredicate(schema.getColumn("dn"), ComparisonOp.EQUAL, dn)
        //声明查询字段
        val list = new util.ArrayList[String]()
        list.add("sitename")
        list.add("siteurl")
        list.add("delete")
        list.add("createtime")
        list.add("creator")
        //查询
        val kuduScanner = kuduClient.newScannerBuilder(dwbBaseWebSiteTable).setProjectedColumnNames(list).addPredicate(eqsiteidPred).addPredicate(eqdnPred).build()
        while (kuduScanner.hasMoreRows) {
          val results = kuduScanner.nextRows()
          while (results.hasNext) {
            val result = results.next()
            sitename = result.getString("sitename")
            siteurl = result.getString("siteurl")
            delete = result.getInt("delete").toString
            site_createtime = result.getString("createtime")
            site_creator = result.getString("creator")
          }
        }
        //将查询到的数据拼装成json格式 存入缓存
        val jsonObject = new JSONObject()
        jsonObject.put("sitename", sitename)
        jsonObject.put("siteurl", siteurl)
        jsonObject.put("delete", delete)
        jsonObject.put("site_createtime", site_createtime)
        jsonObject.put("site_creator", site_creator)
        cache.put("siteDetail:" + siteid + "_" + dn, jsonObject.toJSONString)
      } else {
        //如果缓存中有数据 则解析缓存中的json数据
        val jsonObject = ParseJsonData.getJsonData(siteDetail)
        sitename = jsonObject.getString("sitename")
        siteurl = jsonObject.getString("siteurl")
        delete = jsonObject.getString("delete")
        site_createtime = jsonObject.getString("site_createtime")
        site_creator = jsonObject.getString("site_creator")
      }
    }
    jSONObject.put("sitename", sitename)
    jSONObject.put("siteurl", siteurl)
    jSONObject.put("delete", delete)
    jSONObject.put("site_createtime", site_createtime)
    jSONObject.put("site_creator", site_creator)
    jSONObject
  }

// 查询广告信息表
  def getBaseAd(input: String): JSONObject = {
    val nObject = JSON.parseObject(input)
    //
    val adid = nObject.getInteger("ad_id")
    val dn = nObject.getString("dn")
    var adname ="";
    // 查询对应的cache 看是否命中缓存
    if(null!=adid){
      //
       adname = cache.getIfPresent(adid+"_"+dn)

      if(StringUtils.isEmpty(adname)){ // 如果没有命中 则查询kudu 表
        val schema = dwbBaseadTable.getSchema
        //声明查询条件 等于adid的值
        val eqadidPred = KuduPredicate.newComparisonPredicate(schema.getColumn("adid"), ComparisonOp.EQUAL, adid)
        //什么查询条件 等于dn的值
        val eqdnPred = KuduPredicate.newComparisonPredicate(schema.getColumn("dn"), ComparisonOp.EQUAL, dn)

        val list = new util.ArrayList[String]()
        list.add("adname")
        val kuduScanner = kuduClient.newScannerBuilder(dwbBaseadTable).setProjectedColumnNames(list).addPredicate(eqadidPred)
          .addPredicate(eqdnPred)
          .build()
        while (kuduScanner.hasMoreRows) {
          val results = kuduScanner.nextRows()
          while (results.hasNext) {
            adname = results.next().getString("adname")
            cache.put("adname:" + adid + "_" + dn, adname) //放入缓存
          }
        }
      }
      nObject.put("adname",adname);
      }
      nObject
    }


}
