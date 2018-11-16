package com.dounine.scala.flink.entity

class Log(
              var time: String,
              var appKey: String,
              var ccode: String,
              var scene: String,
              var `type`: String,
              var userId: String
            ) {

  def this() {
    this("", "", "", "", "", "")
  }

  def getTime: String = time

  def getAppKey: String = appKey

  def getCcode: String = ccode

  def getScene: String = scene

  def getType: String = `type`

  def getUserId: String = userId

  def setTime(value: String): Unit = time = value

  def setAppKey(value: String): Unit = appKey = value

  def setCcode(value: String): Unit = ccode = value

  def setScene(value: String): Unit = scene = value

  def setType(value: String): Unit = `type` = value

  def setUserId(value: String): Unit = userId = value

}
