package com.aslepakurov.spark.movies

object MoviesJob {
  def main(args: Array[String]): Unit = {
    val context = new MoviesContext(args.toList)
      .builder
      .disableSuccessFile
      .initContext
      .initSQL
      .get
      .asInstanceOf[MoviesContext]

    try {
      context.validateArgs()
    } catch {
      case e: Throwable =>
        println(e.getMessage)
        throw new RuntimeException(e)
    } finally {
      context.close()
    }
  }
}
