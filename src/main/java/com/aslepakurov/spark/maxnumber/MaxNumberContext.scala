package com.aslepakurov.spark.maxnumber

import com.aslepakurov.spark.CommonSparkContext

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
object MaxNumberContext {
  val NUMBER_STRING = "--numbers"
  val OUTPUT_STRING = "--output"

  def buildArgs(inputNumbers: Option[String], output: Option[String]): Array[String] = {
    var args = Array[String]()
    if (optionPresent(inputNumbers)) args ++= Array(NUMBER_STRING, inputNumbers.get)
    if (optionPresent(output)) args ++= Array(OUTPUT_STRING, output.get)
    args
  }

  def getJobClass: Class[_] = MaxNumberJob.getClass

  private def optionPresent(option: Option[String]): Boolean = {
    option.isDefined && !option.get.trim.equals("")
  }
}
