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
    val outpuFile = files(0)
    Source.fromFile(outpuFile).getLines() should not be null
    Source.fromFile(outpuFile).getLines().length should be (1)
    Source.fromFile(outpuFile).getLines().toList.head should be ("123")
  }

  private def resourceToPath(fileName: String) = {
    new File(getClass.getResource(fileName).toURI).getAbsolutePath
  }
}