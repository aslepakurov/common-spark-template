package com.aslepakurov.spark.example

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.max


object ExampleService {
  def readNumbers(context: ExampleContext): DataFrame = {
    context.readToDF(context.numbersPath.get)
  }

  def getMaxNumber(context: ExampleContext, numbersDF: DataFrame): Row = {
    numbersDF.agg(max("_c0")).collect()(0)
  }
}
