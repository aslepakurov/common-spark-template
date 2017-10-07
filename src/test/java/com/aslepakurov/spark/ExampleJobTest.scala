package com.aslepakurov.spark

import com.aslepakurov.spark.example.{ExampleContext, ExampleJob}
import org.scalatest.{FlatSpec, Matchers}

class ExampleJobTest extends FlatSpec with Matchers {
  "Example job" should "return max number" in {
    ExampleJob.main(ExampleContext.buildArgs(Option("...")))
  }
}
