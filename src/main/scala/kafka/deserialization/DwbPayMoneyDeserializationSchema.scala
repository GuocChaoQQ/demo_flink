package kafka.deserialization

import com.alibaba.fastjson.JSON
import domain.DwdMemberPayMoney
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * created by chao.guo on 2020/7/15
 **/
class DwbPayMoneyDeserializationSchema extends  KafkaDeserializationSchema[DwdMemberPayMoney]{
  override def isEndOfStream(t: DwdMemberPayMoney): Boolean = false

  override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): DwdMemberPayMoney = {
    val value = new String(consumerRecord.value(),"UTF-8")
    JSON.parseObject(value,classOf[DwdMemberPayMoney])

  }

  override def getProducedType: TypeInformation[DwdMemberPayMoney] = {
    Types.GENERIC(classOf[DwdMemberPayMoney])


  }
}
