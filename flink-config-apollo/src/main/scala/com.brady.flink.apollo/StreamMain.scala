package com.brady.flink.apollo

import com.ctrip.framework.apollo.model.{ConfigChange, ConfigChangeEvent}
import com.ctrip.framework.apollo.{Config, ConfigChangeListener, ConfigService}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._


/**
  * @author xianchang.yue
  * @date 2020-10-26 22:21 
  */
object StreamMain {
  def main(args: Array[String]): Unit = {
    System.setProperty("apollo.meta","http://localhost:8080")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(ParameterTool.fromArgs(args))
    env.setParallelism(1)
    env.addSource(new RichSourceFunction[String] {
      var config:Config =_
      val isRunning = true
      override def open(parameters: Configuration): Unit = {
        config = ConfigService.getAppConfig()
        config.addChangeListener(new ConfigChangeListener {
          override def onChange(configChangeEvent: ConfigChangeEvent): Unit = {
            import scala.collection.JavaConversions._
            for (keys: String <- configChangeEvent.changedKeys) {
              val change: ConfigChange = configChangeEvent.getChange(keys)
              println(change.getChangeType + " " + change.getNamespace + " " + change.getNewValue + " " + change.getPropertyName + " " + change.getOldValue)
            }
          }
        })
      }
      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        while (isRunning){
          ctx.collect(config.getProperty("no-sink-parallelism","hello"))
          Thread.sleep(1000)
        }
      }

      override def cancel(): Unit ={
        val isRunning: Boolean = false
      }
    }).print()

    env.execute("flink Apollo")
  }
}
