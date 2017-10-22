package com.aslepakurov.spark.maxnumber

import com.aslepakurov.spark.common.model.JobArgs
import com.aslepakurov.spark.common.{CommonJobContext, CommonSparkContext}
import com.aslepakurov.spark.maxnumber.model.NumberJobArgs

class MaxNumberContext(inputArgs: List[String]) extends CommonSparkContext(inputArgs) {

  import MaxNumberContext._

  def numbersPath: Option[String] = args.get(NUMBER_STRING)

  def outputPath: Option[String] = args.get(OUTPUT_STRING)

  def validateArgs(): Unit = {
    val errorMap = Map(
      numbersPath.isEmpty -> "path to a file with numbers must be specified!",
      outputPath.isEmpty -> "path to output file must be specified!"
    ).filter(_._1)
    if (errorMap.nonEmpty) throw new IllegalArgumentException(printUsageMessage(errorMap.values.toList) + parametersString)
  }

  override def parametersString: String = {
      "\n%s         path to a file with numbers in column (required)\n".format(NUMBER_STRING) +
      "\n%s         path to a write max number to (required)\n".format(OUTPUT_STRING) +
      super.parametersString
  }
}

object MaxNumberContext extends CommonJobContext[NumberJobArgs] {
  val NUMBER_STRING = "--numbers"
  val OUTPUT_STRING = "--output"

  def getJobClass: Class[_] = MaxNumberJob.getClass

  override def buildArgs(jobArgs: JobArgs): Array[String] = {
    jobArgs.getJobArgs
  }
}
