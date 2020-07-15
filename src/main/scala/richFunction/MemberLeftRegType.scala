package richFunction

import java.lang

import com.alibaba.fastjson.JSONObject
import domain.{DwdMember, DwdMemberRegtype}
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.util.Collector

/**
 * created by chao.guo on 2020/7/15
 * 用户表关联上 注册信息表 取出注册时填写的
 *
 **/
class MemberLeftRegType extends  CoGroupFunction[DwdMember,DwdMemberRegtype,String]{
  override def coGroup(left: lang.Iterable[DwdMember], right: lang.Iterable[DwdMemberRegtype], collector: Collector[String]): Unit = {
    var bl = false
    val leftIterator = left.iterator() // 左边的流客户数据
    val rightIterator = right.iterator()// 右表的流注册数据
    while (leftIterator.hasNext){
      val dwdMember = leftIterator.next()
      val jsonObject = new JSONObject()
      jsonObject.put("uid", dwdMember.uid)
      jsonObject.put("ad_id", dwdMember.ad_id)
      jsonObject.put("birthday", dwdMember.birthday)
      jsonObject.put("email", dwdMember.email)
      jsonObject.put("fullname", dwdMember.fullname)
      jsonObject.put("iconurl", dwdMember.iconurl)
      jsonObject.put("lastlogin", dwdMember.lastlogin)
      jsonObject.put("mailaddr", dwdMember.mailaddr)
      jsonObject.put("memberlevel", dwdMember.memberlevel)
      jsonObject.put("password", dwdMember.password)
      jsonObject.put("phone", dwdMember.phone)
      jsonObject.put("qq", dwdMember.qq)
      jsonObject.put("register", dwdMember.register)
      jsonObject.put("regupdatetime", dwdMember.regupdatetime)
      jsonObject.put("unitname", dwdMember.unitname)
      jsonObject.put("userip", dwdMember.userip)
      jsonObject.put("zipcode", dwdMember.zipcode)
      jsonObject.put("dt", dwdMember.dt)
      jsonObject.put("dn", dwdMember.dn)

      while (rightIterator.hasNext){
        bl=true;
        val dwdMemberRegtype = rightIterator.next()
        jsonObject.put("appkey", dwdMemberRegtype.appkey)
        jsonObject.put("appregurl", dwdMemberRegtype.appregurl)
        jsonObject.put("bdp_uuid", dwdMemberRegtype.bdp_uuid)
        jsonObject.put("createtime", dwdMemberRegtype.createtime)
        jsonObject.put("isranreg", dwdMemberRegtype.isranreg)
        jsonObject.put("regsource", dwdMemberRegtype.regsource)
        jsonObject.put("websiteid", dwdMemberRegtype.websiteid)
      }
      if(!bl){ // 没有关联上 则直接保存为空字符串
        jsonObject.put("appkey", "")
        jsonObject.put("appregurl", "")
        jsonObject.put("bdp_uuid", "")
        jsonObject.put("createtime", "")
        jsonObject.put("isranreg", "")
        jsonObject.put("regsource", "")
        jsonObject.put("websiteid", "")
      }
    collector.collect(jsonObject.toJSONString) // 写出数据
    }
  }
}
