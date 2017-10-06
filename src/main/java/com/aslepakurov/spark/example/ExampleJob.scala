package com.aslepakurov.spark.example

import com.aslepakurov.spark.example.ExampleService._

object ExampleJob {
  def main(args: Array[String]): Unit = {
    val context = new ExampleContext(args.toList)
      .builder
      .initContext
      .initSQL
      .enableS3Support
      .disableSuccessFile
      .get
      .asInstanceOf[ExampleContext]

    val numbers = readNumbers(context)

    val maxNumber = getMaxNumber(context, numbers).getAs[String](0)
    println("max umber = %s".format(maxNumber))
  }
}
