package com.mindcandy.waterfall.io

import org.junit.runner.RunWith
import org.specs2.Specification
import org.specs2.mock.Mockito
import org.specs2.runner.JUnitRunner
import com.mindcandy.waterfall.TestFormat
import com.mindcandy.waterfall.Intermediate
import resource.ManagedResource
import com.mindcandy.waterfall.IntermediateFormat
import resource._
import java.io.IOException
import com.github.nscala_time.time.Imports._
import com.mindcandy.waterfall.FileIntermediate
import java.nio.file.Files
import fr.simply.StubServer
import fr.simply.util.ContentType
import scala.io.Source
import com.mindcandy.waterfall.PlainTextFormat
import com.mindcandy.waterfall.MemoryIntermediate
import java.nio.charset.Charset
import scala.collection.JavaConverters._
import com.mindcandy.waterfall.RowSeparator
import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class HttpIOSpec extends Specification with Mockito {
  def is = args.execute(sequential = true) ^ args.report(showtimes = true) ^ s2"""
    HttpIOSouce
      should retrieveFrom with a http url separating lines                     ${HttpIOSouceTests.retrieveWithLinesSeparator}
      should retrieveFrom with a http url not separating lines                 ${HttpIOSouceTests.retrieveWithNoRowSeparator}
      should retrieveFrom receiving no data                                    ${HttpIOSouceTests.retrieveWithNoData}
    MultipleHttpIOSource
      should retrieve data from multiple URLs                                  ${MultipleHttpIOSouceTests.retrieveWithLinesSeparatorFromTwoServers}
  """

  val jsonTestData1 = """|{ "test1" : "value1", "test2" : 45 }
                        |{ "test1" : "value2", "test2" : 67 }""".stripMargin

  val jsonTestData2 = """|{ "testA" : "valueA", "testB" : 12 }
                        |{ "testA" : "valueB", "testB" : 34 }""".stripMargin

  val jsonTestDataNoSeparator = """|{
                                   |"test1" : "value1",
                                   |"test2" : 45
                                   |}""".stripMargin

  def newTempFileUrl() = {
    val file = Files.createTempFile("waterfall-tests-", ".tsv")
    file.toFile.deleteOnExit()
    file.toUri.toString
  }

  object HttpIOSouceTests {
    def retrieveWithLinesSeparator = {
      val server = new StubServer(8090).defaultResponse(ContentType("text/plain"), jsonTestData1, 200).start

      val intermediate = new MemoryIntermediate[PlainTextFormat]("memory:test")
      val vfsIO = HttpIOSource[PlainTextFormat](BaseIOConfig("http://localhost:8090/test"))
      vfsIO.retrieveInto(intermediate)
      server.stop
      intermediate.data must haveTheSameElementsAs(List(List("""{ "test1" : "value1", "test2" : 45 }"""), List("""{ "test1" : "value2", "test2" : 67 }""")))
    }
    def retrieveWithNoRowSeparator = {
      val server = new StubServer(8090).defaultResponse(ContentType("text/plain"), jsonTestDataNoSeparator, 200).start

      val intermediate = new MemoryIntermediate[PlainTextFormat]("memory:test")
      val vfsIO = HttpIOSource[PlainTextFormat](BaseIOConfig("http://localhost:8090/test"), rowSeparator = RowSeparator.NoSeparator)
      vfsIO.retrieveInto(intermediate)
      server.stop
      intermediate.data must haveTheSameElementsAs(List(List("""{"test1" : "value1","test2" : 45}""")))
    }
    def retrieveWithNoData = {
      val server = new StubServer(8090).defaultResponse(ContentType("text/plain"), "", 200).start

      val intermediate = new MemoryIntermediate[PlainTextFormat]("memory:test")
      val vfsIO = HttpIOSource[PlainTextFormat](BaseIOConfig("http://localhost:8090/test"), rowSeparator = RowSeparator.NoSeparator)
      vfsIO.retrieveInto(intermediate)
      server.stop
      intermediate.data must haveTheSameElementsAs(List())
    }
  }

  object MultipleHttpIOSouceTests {
    def retrieveWithLinesSeparatorFromTwoServers = {
      val server1 = new StubServer(8090).defaultResponse(ContentType("text/plain"), jsonTestData1, 200).start
      val server2 = new StubServer(8091).defaultResponse(ContentType("text/plain"), jsonTestData2, 200).start
      
      val intermediate = new MemoryIntermediate[PlainTextFormat]("memory:test")
      val vfsIO = MultipleHttpIOSource[PlainTextFormat](new MultipleHttpIOConfig() {
        def urls = List("http://localhost:8090/test", "http://localhost:8091/test")
        def combinedFileUrl = newTempFileUrl()
      })
      val result = vfsIO.retrieveInto(intermediate)
      server1.stop
      server2.stop
      
      (result must beSuccessfulTry) and {
        intermediate.getData must_== List(List("""{ "test1" : "value1", "test2" : 45 }"""), List("""{ "test1" : "value2", "test2" : 67 }"""), List("""{ "testA" : "valueA", "testB" : 12 }"""), List("""{ "testA" : "valueB", "testB" : 34 }"""))
      }
    }
  }
}