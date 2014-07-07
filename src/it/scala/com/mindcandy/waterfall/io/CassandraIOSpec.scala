package com.mindcandy.waterfall.io

import com.github.nscala_time.time.Imports._
import com.mindcandy.waterfall.intermediate.MemoryIntermediate
import com.mindcandy.waterfall.intermediate.MemoryIntermediate
import com.mindcandy.waterfall.{IntermediateFormat, IntermediateFormatCompanion}
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType
import com.netflix.astyanax.connectionpool.NodeDiscoveryType
import com.netflix.astyanax.model.ConsistencyLevel
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.Specification
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

@RunWith(classOf[JUnitRunner])
class CassandraIOSpec extends Specification {
  override def is = s2"""
    CassandroIO should
      store two lines correctly in a column family $storeTwoLines
      receive two lines correctly from a column family $receiveTwoLines
  """

  case class CassandraTestData(userID: String, joined: DateTime, age: Int, name: String)

  object CassandraTestData extends IntermediateFormatCompanion[CassandraTestData] {
    object format extends IntermediateFormat[CassandraTestData] {
      def convertTo(input: Seq[String]) = {
        CassandraTestData(
          input(0),
          ISODateTimeFormat.dateTime.parseDateTime(input(1)),
          input(2).toInt,
          input(3)
        )
      }

      def convertFrom(input: CassandraTestData) = Seq[String](
        input.userID,
        input.joined.toString(ISODateTimeFormat.dateTime.withZoneUTC()),
        input.age.toString,
        input.name
      )
    }

    val keyField = "userID"
  }

  val clusterConfig = CassandraIOClusterConfig(
    name = "WaterfallTesting",
    seedHosts = "FILLIN_HOST",
    keySpace = "waterfall_testing",
    localDatacenter = ""
  )
  val cassandraConfig = CassandraIOConfig(clusterConfig, "cassandra_io_test", CassandraTestData.keyField)

  def storeTwoLines = {
    val intermediate: MemoryIntermediate[CassandraTestData] = new MemoryIntermediate("memory:source")
    intermediate.write(Iterator(
      CassandraTestData("dg7327fds2a", new DateTime(2013, 10, 30, 23, 11, 23, 0, DateTimeZone.UTC), 35, "Test User One"),
      CassandraTestData("afrds2363", new DateTime(2014, 5, 2, 7, 45, 12, 0, DateTimeZone.UTC), 21, "Test User Two")
    ))
    val cassandraSink = CassandraIO[CassandraTestData](cassandraConfig)
    val result = cassandraSink.storeFrom(intermediate)
    result must beSuccessfulTry
  }
  def receiveTwoLines = pending
}
