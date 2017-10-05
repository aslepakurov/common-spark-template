package com.aslepakurov.spark.example

import com.aslepakurov.spark.CommonSparkContext

class ExampleContext (inputArgs: List[String]) extends CommonSparkContext(inputArgs) {
  import ExampleContext._
  def numbersPath: Option[String] = args.get(NUMBER_STRING)

  def validateArgs(): Unit = {
    val errorMap = Map(
      numbersPath.isEmpty -> "path to a file with numbers must be specified!"
    ).filter(_._1)
    if (errorMap.nonEmpty) throw new IllegalArgumentException(printUsageMessage(errorMap.values.toList) + parametersString)
  }

  override def parametersString: String = {
    "\n%s         path to a file with numbers in column (required)\n".format(NUMBER_STRING) +
      super.parametersString
  }
}
object ExampleContext {
  val NUMBER_STRING = "--numbers"

  def buildArgs(inputNumbers: Option[String]): List[String] = {
    var args = List[String]()
    if (optionPresent(inputNumbers)) args ++= List(NUMBER_STRING, inputNumbers.get)
    args
  }

  def getJobClass: Class[_] = ExampleJob.getClass

  private def optionPresent(option: Option[String]): Boolean = {
    option.isDefined && !option.get.trim.equals("")
  }
}
