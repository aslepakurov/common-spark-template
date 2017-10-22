package com.aslepakurov.spark.common

import com.aslepakurov.spark.common.model.JobArgs

trait CommonJobContext[+T<: JobArgs] {
  def getJobClass: Class[_]

  def buildArgs(jobArgs: JobArgs): Array[String] = {
    jobArgs.getJobArgs
  }
}
