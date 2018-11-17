package com.dounine.scala.flink

import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime}

import com.dounine.scala.flink.App.LOGGER
import com.dounine.scala.flink.entity.Log
import com.dounine.scala.flink.hbase.CustomTableInputFormat
import com.dounine.scala.flink.source.LogSource
import com.dounine.scala.flink.utils.HadoopKrb
import com.dounine.scala.flink.utils.HbaseUtil._
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.hadoopcompatibility.HadoopInputs
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.types.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.mapreduce.Job
import org.slf4j.LoggerFactory

object App {


  val LOGGER = LoggerFactory.getLogger(classOf[App])
  val conf = HadoopKrb.login()

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val rowStart = "181111000000"
    val stopRow = "181111010000"
    conf.set(TableInputFormat.INPUT_TABLE, "logTable")

    conf.set(TableInputFormat.SCAN_ROW_START, rowStart)
    conf.set(TableInputFormat.SCAN_ROW_STOP, stopRow)

    LOGGER.info(s"hbase start ${rowStart} stop ${stopRow}")

    val logDataStream = env.addSource(new LogSource())

    val table = tableEnv.fromDataStream(logDataStream, "appKey,ccode,scene,type,userId,time")

    tableEnv.registerTable("log", table)

    val tt = tableEnv.sqlQuery("select * from log")

//    tableEnv.toAppendStream(tt,classOf[Row]).print()

    val currentTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy_MM_dd'T'HH_mm_ss"))

    tableEnv.toAppendStream(tt,classOf[Row]).writeAsText(s"""hdfs:///tmp/flink/stream1/${currentTime}""", FileSystem.WriteMode.OVERWRITE)

    env.execute
    LOGGER.info("finish")
  }

}

