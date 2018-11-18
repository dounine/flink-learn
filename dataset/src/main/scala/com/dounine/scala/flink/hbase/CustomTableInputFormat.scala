package com.dounine.scala.flink.hbase

import java.io.IOException
import java.net.{InetAddress, InetSocketAddress, UnknownHostException}
import java.{lang, util}

import com.dounine.scala.flink.utils.HadoopKrb
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableSplit}
import org.apache.hadoop.hbase.security.User
import org.apache.hadoop.hbase.util.{Bytes, Strings}
import org.apache.hadoop.hbase.{HRegionLocation, TableName}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext}
import org.apache.hadoop.net.DNS
import org.apache.hadoop.security.UserGroupInformation


class CustomTableInputFormat extends TableInputFormat {

  var reverseDNSCacheMap: util.HashMap[InetAddress, String] = new util.HashMap[InetAddress, String]

  @throws(classOf[IOException])
  override def getSplits(context: JobContext): util.List[InputSplit] = {
    init(context)

    val conf = context.getConfiguration
    val start = conf.get(TableInputFormat.SCAN_ROW_START)
    val end = conf.get(TableInputFormat.SCAN_ROW_STOP)

    val tableName: TableName = super.getTable.getName
    val splits: util.List[InputSplit] = new util.ArrayList[InputSplit]

    val prefexs = (0 until 256).map(Integer.toHexString).map {
      s =>
        if (s.length == 1) "0" + s
        else s
    }

    prefexs.foreach {
      prefex =>
        val location: HRegionLocation = getRegionLocator.getRegionLocation(Bytes.toBytes(prefex))
        val isa: InetSocketAddress = new InetSocketAddress(location.getHostname, location.getPort)
        val regionLocation = reverse(isa.getAddress)

        val splitStart: Array[Byte] = Bytes.toBytes(s"${prefex}|${start}")
        val splitStop: Array[Byte] = Bytes.toBytes(s"${prefex}|${end}")
        val split: TableSplit = new TableSplit(tableName, this.getScan, splitStart, splitStop, regionLocation)

        splits.add(split)
    }
    splits
  }

  @throws(classOf[UnknownHostException])
  def reverse(ipAddress: InetAddress): lang.String = {
    var hostName: lang.String = this.reverseDNSCacheMap.get(ipAddress)
    if (hostName == null) {
      var ipAddressString: String = null
      try {
        ipAddressString = DNS.reverseDns(ipAddress, null)
      }
      catch {
        case e: Exception =>
          ipAddressString = InetAddress.getByName(ipAddress.getHostAddress).getHostName
      }
      if (ipAddressString == null) throw new UnknownHostException("No host found for " + ipAddress)
      hostName = Strings.domainNamePointerToHostName(ipAddressString)
      this.reverseDNSCacheMap.put(ipAddress, hostName)
    }
    hostName
  }

  def init(context: JobContext): Unit = {
    val conf: Configuration = context.getConfiguration
    val tableName: TableName = TableName.valueOf(conf.get(TableInputFormat.INPUT_TABLE))
    try {
      UserGroupInformation.setConfiguration(conf)
      HadoopKrb.login()
      val user = User.create(UserGroupInformation.getLoginUser)
      initializeTable(ConnectionFactory.createConnection(conf, user), tableName)
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }
}
