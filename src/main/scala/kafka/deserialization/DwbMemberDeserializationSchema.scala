package kafka.deserialization

import com.alibaba.fastjson.JSON
import domain.DwdMember
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * created by chao.guo on 2020/7/15
 **/
class DwbMemberDeserializationSchema  extends  KafkaDeserializationSchema[DwdMember]{
  override def isEndOfStream(t: DwdMember): Boolean = false

  override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): DwdMember = {
    val value = new String(consumerRecord.value(),"UTF-8")
    JSON.parseObject(value,classOf[DwdMember])

  }

  override def getProducedType: TypeInformation[DwdMember] = {
    Types.GENERIC(classOf[DwdMember])


  }
}
