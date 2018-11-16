package com.dounine.scala.flink

import java.time.{Duration, LocalDateTime}

import com.dounine.scala.flink.entity.Log
import com.dounine.scala.flink.hbase.CustomTableInputFormat
import com.dounine.scala.flink.utils.HadoopKrb
import com.dounine.scala.flink.utils.HbaseUtil._
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.hadoopcompatibility.HadoopInputs
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.types.Row
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.mapreduce.Job

object App {

  def main(args: Array[String]): Unit = {

    val startTime = LocalDateTime.now
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)


    val conf = HadoopKrb.login()
    conf.set(TableInputFormat.INPUT_TABLE, "logTable")
    conf.set(TableInputFormat.SCAN_ROW_START, "181111000000")
    conf.set(TableInputFormat.SCAN_ROW_STOP, "181111010000")

    val inputFormat = HadoopInputs.createHadoopInput(
      new CustomTableInputFormat,
      classOf[ImmutableBytesWritable],
      classOf[Result],
      Job.getInstance(conf)
    )

    val logDataStream = env.createInput(inputFormat)
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

    val tt = tableEnv.sqlQuery("select count(*) from log")

    tableEnv.toDataSet(tt, classOf[Row]).print()

    println(Duration.between(startTime, LocalDateTime.now()).getSeconds)

    //    env.execute
  }

}

