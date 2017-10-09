package com.aslepakurov.spark

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

/**
  * Created by aslepakurov on 4/10/17.
  */
class CommonSparkContext (inputArgs: List[String]) extends Serializable {
  import CommonSparkContext._

  @transient var sparkContext: SparkContext = _
  @transient var sqlContext: SQLContext = _
  val args: Map[String, String] = parseArguments(inputArgs)

  class Builder(context: CommonSparkContext) {
    @transient private[this] var builder: SparkSession.Builder = SparkSession.builder()

    def initContext: Builder = {
      builder = builder
        .master(args.getOrElse(SPARK_MASTER, DEFAULT_MASTER))
        .appName(args.getOrElse(APP_NAME, DEFAULT_APP_NAME))
      this
    }

    def initSQL: Builder = {
      builder = builder
        .enableHiveSupport()
        .config("spark.sql.parquet.filterPushdown", value = true)
      this
    }

    def withOverhead(config: String): Builder = {
      builder = builder
        .config("spark.yarn.executor.memoryOverhead", config)
      this
    }

    def withDriverMemory(config: String): Builder = {
      builder = builder
        .config("spark.driver.memory", config)
      this
    }

    def withExecutorMemory(config: String): Builder = {
      builder = builder
        .config("spark.executor.memory", config)
      this
    }

    def enableS3Support: Builder = {
      builder = builder
        .config("spark.hadoop.fs.s3.impl", args.getOrElse(S3_IMPL, DEFAULT_S3_IMPL))
        .config("spark.hadoop.fs.s3.awsAccessKeyId", args.getOrElse(AWS_ACCESS_KEY, ""))
        .config("spark.hadoop.fs.s3.awsSecretAccessKey", args.getOrElse(AWS_SECRET_KEY, ""))
      this
    }

    def withSerializer(serializer: String): Builder = {
      builder = builder
        .config("spark.serializer", serializer)
      this
    }

    def disableSuccessFile: Builder = {
      builder = builder.config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", value = false)
      this
    }

    def enableGzip: Builder = {
      builder = builder
        .config("spark.hadoop.mapreduce.output.fileoutputformat.compress", value = true)
        .config("spark.hadoop.mapreduce.output.fileoutputformat.compress.codec", classOf[GzipCodec].getCanonicalName)
      this
    }

    def get: CommonSparkContext = {
      val session = builder.getOrCreate()
      sparkContext = session.sparkContext
      sqlContext = session.sqlContext
      context
    }
  }

  def isLocalMode: Boolean = args.getOrElse(LOCAL, "false").toBoolean

  //not necessary on EMR
  def isAWSEnabled: Boolean = args.get(AWS_ACCESS_KEY).isDefined && args.get(AWS_SECRET_KEY).isDefined

  def builder: Builder = {
    new Builder(this)
  }
  def readToDF(path: String, format: String = "parquet", schema: StructType = null): DataFrame = {
    if (sqlContext == null) throw new IllegalArgumentException("Context is not initialized!")
    if (path == null) throw new IllegalArgumentException("Path should be specified!")
    var read = sqlContext.read
    if(schema != null) read = read.schema(schema)
    read
      .format(format)
      .load(path)
  }

  def writeDFToFile(dataFrame: DataFrame, path: String, format: String = "parquet", partitions: Int = 1, mode: SaveMode = SaveMode.Overwrite): Unit = {
    dataFrame
      .repartition(partitions)
      .write
      .format(format)
      .mode(mode)
      .save(path)
  }

  def printUsageMessage(errors: List[String]): String = {
    "Error%s occurred: %s\n".format(if (errors.size > 1) "s" else "", errors.mkString(","))
  }

  def parametersString: String = {
    "%s         spark application name (default: %s)\n".format(APP_NAME, DEFAULT_APP_NAME) +
    "%s         spark master for application (default: %s)\n".format(SPARK_MASTER, DEFAULT_MASTER) +
    "%s         whether spark running in local mode (default: %s)\n".format(LOCAL, "false") +
    "%s         implementation of S3 interaction class (default: %s)\n".format(S3_IMPL, DEFAULT_S3_IMPL) +
    "%s         access key for AWS interaction (default: %s)\n".format(AWS_ACCESS_KEY, "\"\"") +
    "%s         secret key for AWS interaction (default: %s)\n".format(AWS_SECRET_KEY, "\"\"")
  }

  private def parseArguments(inputArgs: List[String]): Map[String, String] = {
    var args = inputArgs
    var argsMap = Map[String, String]()

    while(args.nonEmpty) {
      args match {
        case key :: value :: tail =>
          argsMap = argsMap + (key -> value)
          args = tail
      }
    }
    argsMap
  }
}
object CommonSparkContext {
  val LOCAL          = "--local"
  val S3_IMPL        = "--s3-impl"
  val APP_NAME       = "--app-name"
  val SPARK_MASTER   = "--spark-master"
  val AWS_ACCESS_KEY = "--aws-access-key"
  val AWS_SECRET_KEY = "--aws-secret-key"

  val DEFAULT_MASTER   = "local[*]"
  val DEFAULT_APP_NAME = "smartbuy-default"
  val DEFAULT_S3_IMPL  = "org.apache.hadoop.fs.s3native.NativeS3FileSystem"
  val DEFAULT_CSV_IMPL = "org.apache.spark.sql.execution.datasources.csv.CSVFileFormat"
}