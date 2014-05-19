package com.mindcandy.waterfall

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import com.github.nscala_time.time.Imports._
import org.specs2.mock.Mockito
import com.amazonaws.services.s3.AmazonS3Client
import java.io.File
import org.mockito.ArgumentCaptor
import org.specs2.mutable.Specification
import org.mockito.Matchers.{eq => meq}

@RunWith(classOf[JUnitRunner])
class IntermediateSpec extends Specification with Mockito {

  "S3 Intermediate" should {
    "write a single small file to S3 with a proper filename" in {
      val intermediate = new S3Intermediate[TestFormat]("s3-eu-west-1.amazonaws.com", "access-key",
        "secret-key", "waterfall-testing", "testfile", new DateTime(2013,10, 1, 0, 0, 0, 0, DateTimeZone.UTC)) {
        override val amazonS3Client = {
          val client = mock[AmazonS3Client]
          client
        }
      }

      val testData = List.tabulate(10000)(n => TestFormat(n, "middleware-" + n, new DateTime(1989, 11, 9, 12, 18, 57, 0, DateTimeZone.UTC)))
      intermediate.write(testData.iterator)
      
      val captureFile = capture[File]
      there was one(intermediate.amazonS3Client).putObject(meq("waterfall-testing"), meq("testfile-20131001-0.tsv"), captureFile)
      captureFile.value must not be (null)
      captureFile.value.length must be_== (457780)
    }
    
    "write a large dataset as two files to S3 with proper filenames" in {
      val intermediate = new S3Intermediate[TestFormat]("s3-eu-west-1.amazonaws.com", "access-key",
        "secret-key", "waterfall-testing", "testfile", new DateTime(2013,10, 1, 0, 0, 0, 0, DateTimeZone.UTC)) {
        override val fileChunkSize = 400000
        override val amazonS3Client = {
          val client = mock[AmazonS3Client]
          client
        }
      }

      val testData = List.tabulate(10000)(n => TestFormat(n, "middleware-" + n, new DateTime(1989, 11, 9, 12, 18, 57, 0, DateTimeZone.UTC)))
      intermediate.write(testData.iterator)
      
      val captureFile = capture[File]
      there was one(intermediate.amazonS3Client).putObject(meq("waterfall-testing"), meq("testfile-20131001-0.tsv"), captureFile)
      captureFile.value must not be (null)
      captureFile.value.length must be_== (400004)
      there was one(intermediate.amazonS3Client).putObject(meq("waterfall-testing"), meq("testfile-20131001-1.tsv"), captureFile)
      captureFile.value must not be (null)
      captureFile.value.length must be_== (57776)
    }
  }
}