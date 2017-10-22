package com.aslepakurov.spark.common.model

import com.aslepakurov.spark.common.CommonSparkContext

abstract class JobArgs {
  def name: String = ""
  def master: String = ""
  def local: String = ""
  def awsAccessKey: String = ""
  def awsSecretKey: String = ""

  def getJobArgs:Array[String] = {
    var args = Array[String]()
    if (optionPresent(name))         args ++= Array(CommonSparkContext.APP_NAME, name)
    if (optionPresent(master))       args ++= Array(CommonSparkContext.SPARK_MASTER, master)
    if (optionPresent(local))        args ++= Array(CommonSparkContext.LOCAL, local)
    if (optionPresent(awsAccessKey)) args ++= Array(CommonSparkContext.AWS_ACCESS_KEY, awsAccessKey)
    if (optionPresent(awsSecretKey)) args ++= Array(CommonSparkContext.AWS_SECRET_KEY, awsSecretKey)
    args
  }

  def optionPresent(option: String): Boolean = {
    option == null && !option.trim.equals("")
  }
}
