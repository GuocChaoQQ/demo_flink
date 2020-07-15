package kafka.consumer

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import domain.{GlobalConfig, TopicAndValue}
import kafka.deserialization.TopicAndValueDeserializationSchema
import kafka.serialization.DwbKafkaSerialization
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.util.Collector
import sink.DimKuduSink
import utils.ParseJsonData

/**
 * created by chao.guo on 2020/7/14
 * 1  flink 实时消费kafka 中的数据  将数据分成维度表和事实表
 * 2 维度表的数据写入到kudu 表里  事实表的数据 写入到下层kafka 中
 *
 *
 **/
object OdsKafkaEtl {
  val BOOTSTRAP_SERVERS = "bootstrap.servers"
  val GROUP_ID = "group.id"
  val RETRIES = "retries"
  val TOPIC = "topic"

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)  // 处理传递的参数
    //val env = StreamExecutionEnvironment.getExecutionEnvironment
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI() //在本地跑flink 程序
    env.getConfig.setGlobalJobParameters(params) // 设置全局的参数
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 设置为eventTime
  // 设置checkpoint
    env.enableCheckpointing(60000L) ;//1分钟做一次checkpoint
    val checkpoint_config = env.getCheckpointConfig
    checkpoint_config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) // 设置为exactly_onece
    checkpoint_config.setMinPauseBetweenCheckpoints(30000l) //设置checkpoint间隔时间30秒
    checkpoint_config.setCheckpointTimeout(100000l) //设置checkpoint超时时间
    checkpoint_config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION) //cancel时保留checkpoint
    //设置statebackend 为rockdb
    //    val stateBackend: StateBackend = new RocksDBStateBackend("hdfs://nameservice1/flink/checkpoint")
    //    env.setStateBackend(stateBackend)
    //设置重启策略   重启3次 间隔10秒
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)))
    import scala.collection.JavaConverters._
    val topicList = params.get(TOPIC).split(",").toBuffer.asJava
    val consumerProps = new Properties()
    consumerProps.setProperty(BOOTSTRAP_SERVERS, params.get(BOOTSTRAP_SERVERS))
    consumerProps.setProperty(GROUP_ID, params.get(GROUP_ID))
    // 消费kafka 中的数据  反序列成 样例类 topicAndValue 类型
    val kafaEventSource: FlinkKafkaConsumer010[TopicAndValue] = new FlinkKafkaConsumer010[TopicAndValue](topicList, new TopicAndValueDeserializationSchema, consumerProps)
    kafaEventSource.setStartFromEarliest() // 设置从最开始的地方开始消费
    implicit val evidence$9:TypeInformation[TopicAndValue] =Types.GENERIC(classOf[TopicAndValue])
    val dataStream: DataStream[TopicAndValue] = env.addSource(kafaEventSource).filter(item => {
      //先过滤非json数据
      val obj = ParseJsonData.getJsonData(item.value)
      obj.isInstanceOf[JSONObject]
    })
    //定义侧输出流 将维度表数据写出到kudu
    // 将事实表的数据 写入到下层kafka
    val dim_out_put_tag=new OutputTag[TopicAndValue]("dim_out_put_tag") // 侧输出流

    val selectDstream = dataStream.process(new ProcessFunction[TopicAndValue, TopicAndValue] {
      override def processElement(value: TopicAndValue, context: ProcessFunction[TopicAndValue, TopicAndValue]#Context, collector: Collector[TopicAndValue]): Unit = {
        value.topic match {
          case "test_ad" | "test_vip" | "test_web" => context.output(dim_out_put_tag, value)
          case _ => collector.collect(value)
        }
      }
    })
    // 获取侧输出流
    val dimStream = selectDstream.getSideOutput(dim_out_put_tag)
    //dimStream.print("dim")
    // 测输出流的数据写出到kudu表
    dimStream.addSink(new DimKuduSink)
    // 事实表的数据写出到kafka
    selectDstream.addSink(new FlinkKafkaProducer010[TopicAndValue](GlobalConfig.BOOTSTRAP_SERVERS,"",new DwbKafkaSerialization))
    //dimStream.print("dwb")
    env.execute("test")














  }



}
