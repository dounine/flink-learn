package com.dounine.scala.flink.source

import java.util.concurrent.TimeUnit

import com.dounine.scala.flink.App
import com.dounine.scala.flink.entity.Log
import com.dounine.scala.flink.hbase.CustomTableInputFormat
import com.dounine.scala.flink.utils.HbaseUtil.getValue
import org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat
import org.apache.flink.api.java.hadoop.mapreduce.wrapper.HadoopInputSplit
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.hadoopcompatibility.HadoopInputs
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.mapreduce.Job

class LogSource() extends SourceFunction[Log] with Serializable {

  var isCancel: Boolean = false

  override def run(ctx: SourceFunction.SourceContext[Log]): Unit = {

    val conf = App.getCConf
    val inputFormat: HadoopInputFormat[ImmutableBytesWritable, Result] = HadoopInputs.createHadoopInput(new CustomTableInputFormat, classOf[ImmutableBytesWritable], classOf[Result], Job.getInstance(conf))
    while (!isCancel) {
      val splits: Array[HadoopInputSplit] = inputFormat.createInputSplits(1)
      for (split <- splits) {
        inputFormat.open(split)
        while ( {
          !inputFormat.reachedEnd
        }) {
          var record: Tuple2[ImmutableBytesWritable, Result] = new Tuple2[ImmutableBytesWritable, Result]
          record = inputFormat.nextRecord(record)
          if (record != null) {
            val result: Result = record.f1
            val v = (qualifier: String) => getValue(record.f1, "ext", qualifier)
            val log = new Log(
              v("time"),
              v("appKey"),
              v("channelCode"),
              v("scene"),
              v("type"),
              v("userId")
            )
            ctx.collect(log)
          }
        }
        inputFormat.close()
        TimeUnit.SECONDS.sleep(10)
      }
    }


  }

  override def cancel(): Unit = {
    isCancel = true
  }
}
