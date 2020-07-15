import java.sql.{Connection, DriverManager, ResultSet}

import org.apache.commons.dbcp.BasicDataSource
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
 * created by chao.guo on 2020/4/28
 **/
class HiveSink (val map: Map[String,String]) extends RichSinkFunction[String]{
  private val driverName = "org.apache.hive.jdbc.HiveDriver"
  var connection: Connection = _;
  var countState: ValueState[Long] = _;
  val batch_Size:Long=100;
  var dataSource: BasicDataSource = _;

  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    val stmt = connection.createStatement
   val res: ResultSet = stmt.executeQuery(value)

    while (res.next()) {
      println(res.getString("due_bill_no"))


    }
    println("执行成功-------------------------------"+res)
  }

  override def open(parameters: Configuration): Unit = {
    dataSource =new BasicDataSource();
    dataSource.setDriverClassName(driverName);
    //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
    dataSource.setUrl("jdbc:hive2://node123:10000/default"); //test为数据库名
    dataSource.setUsername("hive")
    dataSource.setPassword("hive")
    //设置连接池的一些参数
    dataSource.setInitialSize(10);
    dataSource.setMinIdle(2);
    connection= getConnection()
  }

  def getConnection(): Connection = {
    dataSource.getConnection

  }
  override def close(): Unit ={
    connection.close();
  }
}
