package com.aslepakurov.spark.movies.model

import com.aslepakurov.spark.common.model.JobArgs
import com.aslepakurov.spark.movies.MoviesContext

case class MoviesJobArgs(moviePath: String,
                         creditsPath: String) extends JobArgs {
  override def getJobArgs: Array[String] = {
    var args = super.getJobArgs
    if (optionPresent(moviePath))   args ++= Array(MoviesContext.MOVIES_PATH, moviePath)
    if (optionPresent(creditsPath)) args ++= Array(MoviesContext.CREDITS_PATH, creditsPath)
    args
  }
}
