package com.dounine.scala.flink.utils

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation

object HadoopKrb {

  private val user = "admin/admin@dounine.com"
  private val keyPath = "/etc/security/keytabs/admin.keytab"

  def login(): Configuration = {
    val conf = getConf
    try {
      System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")
      System.setProperty("sun.security.krb5.debug", "false")
      UserGroupInformation.setConfiguration(conf)
      UserGroupInformation.loginUserFromKeytab(user, keyPath)
    } catch {
      case ex: IOException =>
        ex.printStackTrace()
    }
    conf
  }

  def getConf: Configuration = {
    val cfg = new Configuration
    cfg.addResource("hbase-site.xml")
    cfg.addResource("core-site.xml")
    cfg.addResource("hdfs-site.xml")
    cfg.addResource("mapred-site.xml")
    cfg
  }
}
