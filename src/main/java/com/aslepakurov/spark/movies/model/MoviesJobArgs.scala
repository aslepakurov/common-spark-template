package com.aslepakurov.spark.movies.model

import com.aslepakurov.spark.common.model.JobArgs
import com.aslepakurov.spark.movies.MoviesContext

case class MoviesJobArgs(moviePath: String, creditsPath: String) extends JobArgs {
  override def getJobArgs: Array[String] = {
    var args = super.getJobArgs
    args ++= Array(MoviesContext.MOVIES_PATH, moviePath)
    args ++= Array(MoviesContext.CREDITS_PATH, creditsPath)
    args
  }
}
