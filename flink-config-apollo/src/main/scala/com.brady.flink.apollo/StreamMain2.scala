package com.brady.flink.apollo

import com.ctrip.framework.apollo.model.{ConfigChange, ConfigChangeEvent}
import com.ctrip.framework.apollo.{Config, ConfigChangeListener}
import org.apache.flink.configuration.Configuration
import com.ctrip.framework.apollo.ConfigService
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat
import org.apache.flink.api.java.utils.ParameterTool
import java.io.ByteArrayInputStream

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

/**
  * @author xianchang.yue
  * @date 2020-10-29 12:42 
  */
object StreamMain2 {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    System.setProperty("apollo.meta", "http://localhost:8080")
    loadConfig(env)

    env.addSource(new RichSourceFunction[String] {
      var configs: Config = _
      val isRunning = true

      override def open(parameters: Configuration): Unit = {
        configs = ConfigService.getAppConfig
        configs.addChangeListener(new ConfigChangeListener {
          override def onChange(configChangeEvent: ConfigChangeEvent): Unit = {
            import scala.collection.JavaConversions._
            for (keys: String <- configChangeEvent.changedKeys) {
              val change: ConfigChange = configChangeEvent.getChange(keys)
              val confi = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
              confi.getConfiguration.setString("no-sink-parallelism", change.getNewValue)
              println(change.getChangeType + " " + change.getNamespace + " " + change.getNewValue + " " + change.getPropertyName + " " + change.getOldValue)
            }
          }
        })
      }

      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        while (isRunning) {
          ctx.collect("yxc------------" + configs.getProperty("no-sink-parallelism", "2"))
          Thread.sleep(3000)
        }
      }

      override def cancel(): Unit = {
      }
    }).map(new RichMapFunction[String, String] {
      var log1: String = _

      override def map(value: String): String = {
        val co = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
        val log = co.getConfiguration.getString("no-sink-parallelism", "5")
        val configs:Config = ConfigService.getAppConfig
        log1 = configs.getProperty("no-sink-parallelism","5")
        configs.addChangeListener(new ConfigChangeListener {
          override def onChange(configChangeEvent: ConfigChangeEvent): Unit = {
            import scala.collection.JavaConversions._
            for (keys: String <- configChangeEvent.changedKeys) {
              val change: ConfigChange = configChangeEvent.getChange(keys)
              log1 = change.getNewValue
            }
          }
        })
        value + "---" + log + "----" + log1
      }
    }).print()

    def loadConfig(env: StreamExecutionEnvironment) {
      // 获取配置，注册到全局
      val configFile = ConfigService.getConfigFile("application", ConfigFileFormat.Properties)
      val config = ParameterTool.fromPropertiesFile(new ByteArrayInputStream(configFile.getContent.getBytes))
      env.getConfig.setGlobalJobParameters(config)
    }

    env.execute("flink Apollo")
  }
}
