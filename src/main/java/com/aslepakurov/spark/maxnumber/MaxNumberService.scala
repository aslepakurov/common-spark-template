package com.aslepakurov.spark.maxnumber

import com.aslepakurov.spark.common.CommonSparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}


object MaxNumberService {
  def readNumbers(context: MaxNumberContext): DataFrame = {
    context.readToDF(context.numbersPath.get, format = CommonSparkContext.DEFAULT_CSV_IMPL, schema = StructType(List(StructField("number", IntegerType))))
  }

  def getMaxNumber(context: MaxNumberContext, numbersDF: DataFrame): DataFrame = {
    numbersDF.agg(max("number"))
  }

  def flushMaxNumber(context: MaxNumberContext, maxNumber: DataFrame) = {
    context.writeDFToFile(maxNumber, context.outputPath.get, "csv")
  }
}
