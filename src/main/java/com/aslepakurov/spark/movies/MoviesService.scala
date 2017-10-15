package com.aslepakurov.spark.movies

import org.apache.spark.sql.DataFrame

object MoviesService {
  def readMovies(context: MoviesContext): DataFrame = {
    context.readToDF(context.moviesPath.get, format = "csv", options = Map("header" -> "true"))
  }

  def readCredits(context: MoviesContext): DataFrame = {
    context.readToDF(context.creditsPath.get, format = "csv", options = Map("header" -> "true"))
  }
}
