package com.aslepakurov.spark.maxnumber

import com.aslepakurov.spark.CommonSparkContext
import com.aslepakurov.spark.maxnumber.MaxNumberService._

object MaxNumberJob {
  def main(args: Array[String]): Unit = {
    val context = new MaxNumberContext(args.toList)
      .builder
      .initContext
      .initSQL
      .enableS3Support
      .disableSuccessFile
      .withDriverMemory("1g")
      .withOverhead("1g")
      .withExecutorMemory("1g")
      .withSerializer(CommonSparkContext.DEFAULT_SERIALIZER)
      .get
      .asInstanceOf[MaxNumberContext]

    try {
      context.validateArgs()
      //1. Read numbers from input file
      val numbers = readNumbers(context)
      //2. Get max number
      val maxNumber = getMaxNumber(context, numbers)
      //3. Output max number
      flushMaxNumber(context, maxNumber)
    } catch {
      case e: Throwable => println(e.getMessage)
    } finally {
      context.close()
    }
  }
}
