package kafka.deserialization

import com.alibaba.fastjson.JSON
import domain.{DwdMember, DwdMemberRegtype}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * created by chao.guo on 2020/7/15
 **/
class DwbMemberRegTypeDeserializationSchema extends  KafkaDeserializationSchema[DwdMemberRegtype]{
  override def isEndOfStream(t: DwdMemberRegtype): Boolean = false

  override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): DwdMemberRegtype = {
    val value = new String(consumerRecord.value(),"UTF-8")
    JSON.parseObject(value,classOf[DwdMemberRegtype])

  }

  override def getProducedType: TypeInformation[DwdMemberRegtype] = {
    Types.GENERIC(classOf[DwdMemberRegtype])


  }
}
