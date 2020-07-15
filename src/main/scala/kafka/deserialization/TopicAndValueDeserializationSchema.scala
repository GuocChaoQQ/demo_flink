package kafka.deserialization

import domain.TopicAndValue
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation, Types}
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * created by chao.guo on 2020/7/15
 * kafka 中数据 反序列化类
 *
 *
 **/
class TopicAndValueDeserializationSchema extends  KafkaDeserializationSchema[TopicAndValue]{
  // 是否是最后一条数据 数据是无界的设置为false
  override def isEndOfStream(t: TopicAndValue): Boolean = false

  override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): TopicAndValue = {
      TopicAndValue(consumerRecord.topic(),new String(consumerRecord.value(),"UTF-8"))
  }
  //告诉flink 数据类型
  override def getProducedType: TypeInformation[TopicAndValue] = {
//    TypeInformation.of(new TypeHint[TopicAndValue] {})
    Types.GENERIC(classOf[TopicAndValue])

//    Types.GENERIC(classOf[TopicAndValue])

  }
}
