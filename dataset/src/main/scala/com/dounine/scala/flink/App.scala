package com.dounine.scala.flink

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.dounine.scala.flink.entity.Log
import com.dounine.scala.flink.hbase.CustomTableInputFormat
import com.dounine.scala.flink.utils.HadoopKrb
import com.dounine.scala.flink.utils.HbaseUtil._
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.hadoopcompatibility.HadoopInputs
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.types.Row
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.mapreduce.Job
import org.slf4j.LoggerFactory

object App {
  val LOGGER = LoggerFactory.getLogger(classOf[App])

  def main(args: Array[String]): Unit = {


    val conf = HadoopKrb.login()
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)


    val rowStart = "181111000000"
    val stopRow = "181111010000"
    conf.set(TableInputFormat.INPUT_TABLE, "logTable")

    conf.set(TableInputFormat.SCAN_ROW_START, rowStart)
    conf.set(TableInputFormat.SCAN_ROW_STOP, stopRow)

    LOGGER.info(s"hbase start ${rowStart} stop ${stopRow}")

    val inputFormat = HadoopInputs.createHadoopInput(
      new CustomTableInputFormat,
      classOf[ImmutableBytesWritable],
      classOf[Result],
      Job.getInstance(conf)
    )

    val tupleInfo = createTypeInformation[Tuple2[ImmutableBytesWritable, Result]]

    val logDataStream = env.createInput(inputFormat, tupleInfo)
      .map(new MapFunction[Tuple2[ImmutableBytesWritable, Result], Log]() {
        @throws[Exception]
        override def map(value: Tuple2[ImmutableBytesWritable, Result]): Log = {
          val v = (qualifier: String) => getValue(value.f1, "ext", qualifier)
          new Log(
            v("time"),
            v("appKey"),
            v("channelCode"),
            v("scene"),
            v("type"),
            v("userId")
          )
        }
      })

    val table = tableEnv.fromDataSet(logDataStream, "appKey,ccode,scene,type,userId,time")

    tableEnv.registerTable("log", table)

    val tt = tableEnv.sqlQuery(
      s"""select appKey,ccode,scene,COUNT(DISTINCT userId) from log
         |GROUP BY appKey,ccode,scene""".stripMargin)

    //    tableEnv.toDataSet(tt, classOf[Row]).print()

    tableEnv.toDataSet(tt, classOf[Row]).writeAsText(s"""hdfs://storm5.starsriver.cn:8020/tmp/flink/${LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy_MM_dd'T'HH_mm_ss"))}""")

    env.execute
    LOGGER.info("finish")
  }

}

