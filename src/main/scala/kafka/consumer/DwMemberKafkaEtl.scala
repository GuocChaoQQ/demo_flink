package kafka.consumer

import java.util.Properties

import domain.{DwdMember, DwdMemberPayMoney, DwdMemberRegtype}
import kafka.consumer.OdsKafkaEtl.{BOOTSTRAP_SERVERS, GROUP_ID, TOPIC}
import kafka.deserialization.{DwbMemberDeserializationSchema, DwbMemberRegTypeDeserializationSchema, DwbPayMoneyDeserializationSchema}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import richFunction.{DimKuduAsyncFunction, MemberLeftRegType, MemberRegTypeLeftjoinPayMoney}
import sink.ResultDwbKuduSink
import utils.ParseJsonData


/**
 * created by chao.guo on 2020/7/15
 * //先用户表左关联注册表
 * 再左关联上支付金额表
 *
 *
 **/
object DwMemberKafkaEtl {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)  // 处理传递的参数
  //val env = StreamExecutionEnvironment.getExecutionEnvironment
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
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
    val consumerProps = new Properties()
    consumerProps.setProperty(BOOTSTRAP_SERVERS, params.get(BOOTSTRAP_SERVERS))
    consumerProps.setProperty(GROUP_ID, params.get(GROUP_ID))
    //先加载用户表的数据
    val kafkaMemberSource = new FlinkKafkaConsumer010[DwdMember]("dwb_test_member",new DwbMemberDeserializationSchema(),consumerProps)
    val kafkaMemberPayMoneySource = new FlinkKafkaConsumer010[DwdMemberPayMoney]("dwb_test_pay",new DwbPayMoneyDeserializationSchema(),consumerProps)
    val kafkaMemberRegTypeSource = new FlinkKafkaConsumer010[DwdMemberRegtype]("dwb_test_regtype",new DwbMemberRegTypeDeserializationSchema(),consumerProps)
    kafkaMemberSource.setStartFromEarliest()
    kafkaMemberPayMoneySource.setStartFromEarliest()
    kafkaMemberRegTypeSource.setStartFromEarliest()
    implicit val evidence$9:TypeInformation[DwdMember] =Types.GENERIC(classOf[DwdMember])
    //指定每个Dstream的时间 设置水位线为10秒
    // 用户使用注册时间做eventtime
    val memberDsteam: DataStream[DwdMember] = env.addSource(kafkaMemberSource).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[DwdMember](org.apache.flink.streaming.api.windowing.time.Time.seconds(10)) {
      override def extractTimestamp(t: DwdMember): Long = {
        t.register.toLong

      }
    })
    implicit val evidence$10:TypeInformation[DwdMemberPayMoney] =Types.GENERIC(classOf[DwdMemberPayMoney])
    // 支付金额表 使用 创建时间
    val memberPayMoneyDstream: DataStream[DwdMemberPayMoney] = env.addSource(kafkaMemberPayMoneySource).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[DwdMemberPayMoney](org.apache.flink.streaming.api.windowing.time.Time.seconds(10)) {
      override def extractTimestamp(t: DwdMemberPayMoney): Long = {
        t.createtime.toLong
      }
    })

    // 注册信息表
    implicit val evidence$11:TypeInformation[DwdMemberRegtype] =Types.GENERIC(classOf[DwdMemberRegtype])
    val memberRegTypeDstream: DataStream[DwdMemberRegtype] = env.addSource(kafkaMemberRegTypeSource).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[DwdMemberRegtype](org.apache.flink.streaming.api.windowing.time.Time.seconds(10)) {
      override def extractTimestamp(t: DwdMemberRegtype): Long = {
        t.createtime.toLong
      }
    })

    //    用户表先关联注册表 以用户表为主表 用cogroup 实现left join

    implicit val evidence$12:TypeInformation[String] =Types.GENERIC(classOf[String])
    val dwdmemberLeftJoinRegtyeStream =memberDsteam.coGroup(memberRegTypeDstream)
      .where(it=>it.uid+"_"+it.dn).equalTo(item=>item.uid+"_"+item.dn) // 左表流的 用户id +站点 = 右边流的 用户id+站点
      .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.minutes(10)))
      .trigger(CountTrigger.of(1)) // 触发器 来一条数据触发一条  防止数据丢失  左表数据来一条打印一条  改为2 及在多条数据来的视乎才触发 会变成inner join 数据会被过滤掉
      .apply(new MemberLeftRegType)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](org.apache.flink.streaming.api.windowing.time.Time.seconds(10)) { //
        override def extractTimestamp(element: String): Long = {
          val register = ParseJsonData.getJsonData(element).getString("register") // 取客户表的注册时间作为事件时间
          register.toLong
        }
      })
    // 拿join 完的用户表和支付金额表相关连  取出 对应的支付金额信息
    val resultDstream = dwdmemberLeftJoinRegtyeStream.coGroup(memberPayMoneyDstream)
      .where(it => {
        val nObject = ParseJsonData.getJsonData(it)
        val uid = nObject.get("uid")
        val dn = nObject.get("dn")
        uid + "_" + dn
      }).equalTo(item => item.uid + "_" + item.dn)
      .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.minutes(10)))
      .trigger(CountTrigger.of(1))
      .apply(new MemberRegTypeLeftjoinPayMoney)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](org.apache.flink.streaming.api.windowing.time.Time.seconds(10)) { //
        override def extractTimestamp(element: String): Long = {
          val register = ParseJsonData.getJsonData(element).getString("register") // 取客户表的注册时间作为事件时间
          register.toLong
        }
      })
    //resultDstream.print()

    // 使用异步io 关联kudu 表里的数据

    val resDstream: DataStream[String] = AsyncDataStream.unorderedWait(resultDstream, new DimKuduAsyncFunction, 10l, java.util.concurrent.TimeUnit.MINUTES, 6)
    // 并行度设置为12
    resDstream.print("left join dim table")
 // 把数据写入到kudu 表
    resDstream.addSink(new ResultDwbKuduSink())

    env.execute("test")


















  }
}
