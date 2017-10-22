package com.aslepakurov.spark.maxnumber.model

import com.aslepakurov.spark.common.model.JobArgs
import com.aslepakurov.spark.maxnumber.MaxNumberContext

case class NumberJobArgs(inputNumbers: String, output: String) extends JobArgs {
  override def getJobArgs: Array[String] = {
    var args = super.getJobArgs
    args ++= Array(MaxNumberContext.NUMBER_STRING, inputNumbers)
    args ++= Array(MaxNumberContext.OUTPUT_STRING, output)
    args
  }
}
