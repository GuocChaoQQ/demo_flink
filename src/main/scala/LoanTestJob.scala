
import java.util

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import test.LoanGennerSource

import scala.actors.threadpool.TimeUnit
import scala.util.parsing.json.JSONObject
object LoanTestJob {

  def main(args: Array[String]): Unit = {
    val hiveDDlTag:OutputTag[String] = new OutputTag[String]("hive_ddl")
val hdfs_path ="/home/admin/guo/flink";

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(3000)
    val source = new LoanGennerSource("E:\\data\\11.json")
    // 写hdfs
    // 创建hive 外部表
    //

    val loanSource = env.addSource(source)
    loanSource.uid("hdfs-sink")


    val hdfsSink: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(new Path("home/admin/guo/"), new SimpleStringEncoder[String]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
          .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
          .withMaxPartSize(1024 * 1024 * 1024)
          .build())
      .build()
    loanSource.addSink(hdfsSink)
    import  scala.collection.JavaConverters._
    val hiveDDl = loanSource.map(fun = it => {
      val jsonData = JSON.parseObject(it).getJSONObject("content")
      val dataBase = jsonData.getString("Database")
      val tableName = jsonData.getString("Table")
      val dataJson = jsonData.getJSONObject("Data")
      val buffer: StringBuffer = new StringBuffer()
      buffer.append("create external table if not exists defalut." + dataBase + "_" + tableName + "(").append("\n")
      val jsonKeys = dataJson.keySet().asScala.toList
      jsonKeys.foreach(key => {
        //val value = dataJson.get(key)
        buffer.append(key).append("\t").append("String").append(",").append("\n")
      })
      buffer.toString.substring(0, buffer.toString.lastIndexOf(","))
      buffer.append(") ").append("\n").append("STORED AS textFile location '" + hdfs_path + "/" + dataBase + "_" + tableName + "';")

      (dataBase+"_"+tableName, buffer.toString)
    })
    val value: KeyedStream[(String, String), String] = hiveDDl.keyBy(it=>it._1)
    value.process(new hiveDDlProcessFunction).addSink(new HiveSink(null))

    env.fromCollection(Set("default.test_phone").iterator)


    //loanSource.print()
    //loanSource.addSink(new HiveSink(null))



    env.execute("LoanTestJob")
  }
}

class hiveDDlProcessFunction extends  KeyedProcessFunction[String,(String,String),String]{
// 维护一个Map[String,String] 结构的状态 用作数据去重
  var mapState:MapState[String,String]=_;

  override def open(parameters: Configuration): Unit = {
    val ddl_sql_Descriptor = new MapStateDescriptor("ddl_sql",classOf[String],classOf[String])
    mapState = getRuntimeContext.getMapState(ddl_sql_Descriptor)
  }
  // 数据去重
  override def processElement(i: (String, String), context: KeyedProcessFunction[String, (String, String), String]#Context, collector: Collector[String]): Unit = {
    if(!mapState.contains(i._1)){
      mapState.put(i._1,i._2)
      collector.collect(i._2)
    }
  }



}