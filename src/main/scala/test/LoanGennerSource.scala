package test

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.io.Source

/**
 * created by chao.guo on 2020/4/24
 **/

class LoanGennerSource(val fileUrl:String) extends SourceFunction[String]{
  var source: Source = _
  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    source =Source.fromFile(fileUrl)
    val lines = source.getLines()
    for (elem <- lines) {
      sourceContext.collect(elem)
    }
  }

  override def cancel(): Unit = ???
}
