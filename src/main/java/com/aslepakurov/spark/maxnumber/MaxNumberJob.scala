package com.aslepakurov.spark.maxnumber

import com.aslepakurov.spark.maxnumber.MaxNumberService._

object MaxNumberJob {
  def main(args: Array[String]): Unit = {
    val context = new MaxNumberContext(args.toList)
      .builder
      .initContext
      .initSQL
      .enableS3Support
      .disableSuccessFile
      .get
      .asInstanceOf[MaxNumberContext]

    //1. Read numbers from input file
    val numbers = readNumbers(context)
    //2. Get max number
    val maxNumber = getMaxNumber(context, numbers)
    //3. Output max number
    flushMaxNumber(context, maxNumber)
  }
}