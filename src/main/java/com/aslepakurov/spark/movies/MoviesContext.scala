package com.aslepakurov.spark.movies

import com.aslepakurov.spark.CommonSparkContext

class MoviesContext(inputArgs: List[String]) extends CommonSparkContext(inputArgs) {
  import MoviesContext._

  def moviesPath: Option[String] = args.get(MOVIES_PATH)
  def creditsPath: Option[String] = args.get(CREDITS_PATH)

  def validateArgs(): Unit = {
    val errorMap = Map(
      moviesPath.isEmpty -> "path to a movies file must be specified!",
      creditsPath.isEmpty -> "path to credits file must be specified!"
    ).filter(_._1)
    if (errorMap.nonEmpty) throw new IllegalArgumentException(printUsageMessage(errorMap.values.toList) + parametersString)
  }

  override def parametersString: String = {
      "\n%s         path to a movies file (required)\n".format(MOVIES_PATH) +
      "\n%s         path to a credits file (required)\n".format(CREDITS_PATH) +
      super.parametersString
  }
}
object MoviesContext {
  val MOVIES_PATH  = "--movies-path"
  val CREDITS_PATH = "--credits-path"

  def buildArgs(moviePath: Option[String], creditsPath: Option[String]): Array[String] = {
    var args = Array[String]()
    if (optionPresent(moviePath)) args ++= Array(MOVIES_PATH, moviePath.get)
    if (optionPresent(creditsPath)) args ++= Array(CREDITS_PATH, creditsPath.get)
    args
  }

  def getJobClass: Class[_] = MoviesJob.getClass

  //TODO: extract to trait
  private def optionPresent(option: Option[String]): Boolean = {
    option.isDefined && !option.get.trim.equals("")
  }
}