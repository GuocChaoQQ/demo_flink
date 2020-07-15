package kafka.serialization

import java.nio.charset.Charset

import domain.TopicAndValue
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema

/**
 * created by chao.guo on 2020/7/15
 **/
class DwbKafkaSerialization extends  KeyedSerializationSchema[TopicAndValue]{
  // 序列化key
  override def serializeKey(t: TopicAndValue): Array[Byte] = null
//序列化value
  override def serializeValue(t: TopicAndValue): Array[Byte] = {
  t.value.getBytes(Charset.forName("utf-8"))

  }
// 获取目标topic
  override def getTargetTopic(t: TopicAndValue): String = {
    "dwb_"+t.topic
  }
}
