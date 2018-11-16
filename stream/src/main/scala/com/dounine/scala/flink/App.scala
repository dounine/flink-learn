package com.dounine.scala.flink

import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime}

import com.dounine.scala.flink.entity.Log
import com.dounine.scala.flink.hbase.CustomTableInputFormat
import com.dounine.scala.flink.source.LogSource
import com.dounine.scala.flink.utils.HadoopKrb
import com.dounine.scala.flink.utils.HbaseUtil._
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.hadoopcompatibility.HadoopInputs
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.types.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.mapreduce.Job

object App {


  def getCConf: Configuration ={
    val conf = HadoopKrb.login()
    conf.set(TableInputFormat.INPUT_TABLE, "logTable")
    conf.set(TableInputFormat.SCAN_ROW_START, "181111000000")
    conf.set(TableInputFormat.SCAN_ROW_STOP, "181111000010")
    conf
  }

  def main(args: Array[String]): Unit = {
    val conf = getCConf

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)


    val inputFormat = HadoopInputs.createHadoopInput(
      new CustomTableInputFormat,
      classOf[ImmutableBytesWritable],
      classOf[Result],
      Job.getInstance(conf)
    )

    val logDataStream = env.addSource(new LogSource())

    val table = tableEnv.fromDataStream(logDataStream, "appKey,ccode,scene,type,userId,time")

    tableEnv.registerTable("log", table)

    val tt = tableEnv.sqlQuery("select * from log")

//    tableEnv.toAppendStream(tt,classOf[Row]).print()
    
    tableEnv.toAppendStream(tt,classOf[Row]).writeAsText(s"""hdfs://storm5.starsriver.cn:8020/tmp/flink/stream1""")

    env.execute
  }

}

