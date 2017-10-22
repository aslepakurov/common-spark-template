package com.aslepakurov.spark.common.model

import com.aslepakurov.spark.common.CommonSparkContext

case class JobArgs(name: String = CommonSparkContext.DEFAULT_APP_NAME,
                   master: String = CommonSparkContext.DEFAULT_MASTER,
                   local: String = "false",
                   awsAccessKey: String = "",
                   awsSecretKey: String = "") {
  def getJobArgs:Array[String] = {
    var args = Array[String]()
    args ++= Array(CommonSparkContext.APP_NAME, name)
    args ++= Array(CommonSparkContext.SPARK_MASTER, master)
    args ++= Array(CommonSparkContext.LOCAL, local)
    args ++= Array(CommonSparkContext.AWS_ACCESS_KEY, awsAccessKey)
    args ++= Array(CommonSparkContext.AWS_SECRET_KEY, awsSecretKey)
    args
  }

}
