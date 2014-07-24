package com.mindcandy.waterfall.intermediate

import com.amazonaws.services.s3.AmazonS3Client
import com.github.nscala_time.time.Imports._
import com.mindcandy.waterfall.TestFormat
import java.io.File
import org.mockito.Matchers.{ eq => meq }
import org.specs2.mock.Mockito
import org.specs2.specification.Grouped
import org.specs2.specification.script.Specification

/*
 * test data and constant values of the test data
 */
object TestData {
  // the iterator of data to be written
  def iterator() = {
    val data = List.tabulate(10000)(n => TestFormat(n, "middleware-" + n, new DateTime(1989, 11, 9, 12, 18, 57, 0, DateTimeZone.UTC)))
    data.iterator
  }

  // the size of the data
  val dataSize = 457780
}

class S3IntermediateSpec extends Specification with Grouped with Mockito {
  def is = s2"""
  S3 Intermediate should

  write a single small file to S3 with a proper file name
  =======================================================
    file should not be empty        ${singleFile.e1}
    file should have size of 457780 ${singleFile.e2}

  write a large data set as two files to S3 with proper file names
  ================================================================
    1st file should not be empty        ${twoFiles.e1}
    1st file should have size of 400004 ${twoFiles.e2}
    2nd file should not be empty        ${twoFiles.e3}
    2nd file should have size of 57776  ${twoFiles.e4}
  """

  val singleFile = new group {
    val intermediate = new S3Intermediate[TestFormat]("s3-eu-west-1.amazonaws.com", "access-key",
      "secret-key", "waterfall-testing", "testfile", new DateTime(2013, 10, 1, 0, 0, 0, 0, DateTimeZone.UTC)) {
      override val amazonS3Client = mock[AmazonS3Client]
    }
    intermediate.write(TestData.iterator)

    val captureFile = capture[File]
    there was one(intermediate.amazonS3Client).putObject(meq("waterfall-testing"), meq("testfile-20131001-0.tsv"), captureFile)
    e1 := captureFile.value must not(beNull)
    e2 := captureFile.value.length must be_==(TestData.dataSize)
  }

  val twoFiles = new group {
    val intermediate = new S3Intermediate[TestFormat]("s3-eu-west-1.amazonaws.com", "access-key",
      "secret-key", "waterfall-testing", "testfile", new DateTime(2013, 10, 1, 0, 0, 0, 0, DateTimeZone.UTC)) {
      override val fileChunkSize = 400000
      override val amazonS3Client = mock[AmazonS3Client]
    }

    intermediate.write(TestData.iterator)

    val captureFileFirst = capture[File]
    there was one(intermediate.amazonS3Client).putObject(meq("waterfall-testing"), meq("testfile-20131001-0.tsv"), captureFileFirst)
    val fileFistSize = 400004
    e1 := captureFileFirst.value must not be (null)
    e2 := captureFileFirst.value.length must be_==(fileFistSize)
    val captureFileSecond = capture[File]
    there was one(intermediate.amazonS3Client).putObject(meq("waterfall-testing"), meq("testfile-20131001-1.tsv"), captureFileSecond)
    e3 := captureFileSecond.value must not be (null)
    e4 := captureFileSecond.value.length must be_==(TestData.dataSize - fileFistSize)
  }
}


class FileIntermediateSpec extends Specification with Grouped {
  def is = s2"""
  File Intermediate should

  write a single file
  =======================================================
    file exists                       ${singleFile.e1}
    file should have size of 457780   ${singleFile.e2}

  write to a file which does not exist
  ====================================
    file exists                     ${fileNotExists.e1}
    file should have size of 457780 ${fileNotExists.e2}

  append to an existing file
  =======================================================
    file should have size of 457780*2 ${appendToFile.e1}
  """

  val singleFile = new group {
    // get a temporary file path and url
    val testFile = File.createTempFile("test", "")
    val pathFile = testFile.getAbsolutePath
    val pathURL = testFile.toURI.toURL.toString

    val intermediate = new FileIntermediate[TestFormat](pathURL)
    intermediate.write(TestData.iterator)

    e1 := testFile.exists must beTrue
    e2 := testFile.length must be_==(TestData.dataSize)
  }

  val fileNotExists = new group {
    val testFile = File.createTempFile("test", "")
    val pathFile = testFile.getAbsolutePath
    val pathURL = testFile.toURI.toURL.toString

    // make sure the file not exists
    testFile.delete

    val intermediate = new FileIntermediate[TestFormat](pathURL)
    intermediate.write(TestData.iterator)

    e1 := testFile.exists must beTrue
    e2 := testFile.length must be_==(TestData.dataSize)
  }

  val appendToFile = new group {
    val testFile = File.createTempFile("test", "")
    val pathFile = testFile.getAbsolutePath
    val pathURL = testFile.toURI.toURL.toString

    val intermediate = new FileIntermediate[TestFormat](pathURL)
    intermediate.write(TestData.iterator)
    intermediate.write(TestData.iterator)

    e1 := testFile.length must be_==(TestData.dataSize * 2)
  }
}
