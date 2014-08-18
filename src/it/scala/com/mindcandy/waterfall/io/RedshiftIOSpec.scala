package com.mindcandy.waterfall.io

import com.mindcandy.waterfall._
import com.mindcandy.waterfall.drop.PassThroughWaterfallDrop
import com.mindcandy.waterfall.intermediate.S3Intermediate
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RedshiftIOSpec extends Specification {

  val s3Url = "FILLIN"
  val awsAccessKey = "FILLIN"
  val awsSecretKey = "FILLIN"
  val bucketName = "FILLIN"
  val keyPrefix = "test-file"
  val redshiftUrl = "FILLIN"
  val redshiftUser = "FILLIN"
  val redshiftPass = "FILLIN"
  val tableName = "test_table"
  val query =
    """
      |SELECT * FROM test_table
    """.stripMargin

  case class TestData(userID: String, otherID: String, unifiedID: String, providerID: String, providerLogin: String, timestamp: String)

  object TestData extends IntermediateFormatCompanion[TestData] {
    object format extends IntermediateFormat[TestData] {
      def convertTo(input: Seq[String]) = {
        TestData(
          input(0),
          input(1),
          input(2),
          input(3),
          input(4),
          input(5)
        )
      }

      def convertFrom(input: TestData) = Seq[String](
        input.userID,
        input.otherID,
        input.unifiedID,
        input.providerID,
        input.providerLogin,
        input.timestamp
      )
    }

    val redshiftColumns = List("user_id", "other_id", "unified_id", "provider_id", "provider_login", "timestamp")
  }

  case class TestRedshiftDrop()(implicit val sharedIntermediateFormat: IntermediateFormat[TestData]) extends PassThroughWaterfallDrop[TestData] {
    override val sharedIntermediate = S3Intermediate[TestData](s3Url, awsAccessKey, awsSecretKey, bucketName, keyPrefix)

    override def source = RedshiftIOSource[TestData](
      RedshiftIOSourceConfig(
        redshiftUrl,
        redshiftUser,
        redshiftPass,
        query
      )
    )

    override def sink = RedshiftIOSink[TestData](
      RedshiftIOSinkConfig(
        redshiftUrl,
        redshiftUser,
        redshiftPass,
        tableName,
        Some(TestData.redshiftColumns)
      )
    )
  }

  "RedshiftIOSource" should {
    "should work for test table" in {
      val result = TestRedshiftDrop().run
      result must beSuccessfulTry
    }
  }
}