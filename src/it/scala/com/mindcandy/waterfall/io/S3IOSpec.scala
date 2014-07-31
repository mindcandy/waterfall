package com.mindcandy.waterfall.io

import com.mindcandy.waterfall.PlainTextFormat
import com.mindcandy.waterfall.intermediate.MemoryIntermediate
import org.junit.runner.RunWith
import org.specs2.Specification
import org.specs2.runner.JUnitRunner

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class S3IOSpec extends Specification {
  override def is = s2"""
    S3IO should
      read one file from a bucket $retrieveOneFile
      read multiple files from a bucket $retrieveMultipleFiles
  """

  val s3Config = S3IOConfig(
    url = "s3-eu-west-1.amazonaws.com",
    awsAccessKey = "FILLIN",
    awsSecretKey = "FILLIN",
    bucketName = "waterfall-test-bucket",
    keyPrefix = "logs/"
  )
  val expectedFile = "./test.txt"

  def retrieveOneFile = todo

  def retrieveMultipleFiles = {
    val intermediate: MemoryIntermediate[PlainTextFormat] = new MemoryIntermediate("memory:sink")
    val s3IO = S3IO[PlainTextFormat](s3Config, Some("2014-07-13"))
    val result = s3IO.retrieveInto(intermediate)
    val data = intermediate.getData().map(_(0)).mkString("\n")
    val expected = Source.fromFile(expectedFile).getLines().mkString("\n")
    (result must beSuccessfulTry) and
      (data must_== expected)
  }
}