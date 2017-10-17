package com.aslepakurov.spark

import java.io.File

import com.aslepakurov.spark.maxnumber.{MaxNumberContext, MaxNumberJob}
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

class MaxNumberJobTest extends FlatSpec with Matchers {

  "Max number job" should "return max number" in {
    val testDirectory = File.createTempFile("test", "null")
    MaxNumberJob.main(MaxNumberContext.buildArgs(Option(resourceToPath("/numbers.csv")), Option(testDirectory.getAbsolutePath)))
    val files = testDirectory.listFiles().filter(_.getName.startsWith("part"))
    files.length should be (1)
    val outputFile = files(0)
    Source.fromFile(outputFile).getLines() should not be null
    Source.fromFile(outputFile).getLines().length should be (1)
    Source.fromFile(outputFile).getLines().toList.head should be ("123")
  }

  "Max number job class" should "be relevant" in {
    MaxNumberContext.getJobClass should be (MaxNumberJob.getClass)
  }

  ignore should "throw IllegalArgumentException" in {
    val thrown = intercept[IllegalArgumentException] {
      MaxNumberJob.main(Array(MaxNumberContext.NUMBER_STRING, "/empty-path"))
    }
    thrown.getMessage should not be empty
  }

  private def resourceToPath(fileName: String) = {
    new File(getClass.getResource(fileName).toURI).getAbsolutePath
  }
}
