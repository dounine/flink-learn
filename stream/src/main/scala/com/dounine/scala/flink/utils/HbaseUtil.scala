package com.dounine.scala.flink.utils

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

object HbaseUtil {

  def getValue(result: Result, family: String, qualifier: String): String = {
    Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)))
  }

}
