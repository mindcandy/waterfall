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
import fr.simply.GET
import fr.simply.DynamicServerResponse
import fr.simply.StaticServerResponse
import java.net.SocketTimeoutException

@RunWith(classOf[JUnitRunner])
class HttpIOSpec extends Specification with Mockito {
  def is = args.execute(sequential = true) ^ args.report(showtimes = true) ^ s2"""
    HttpIOSouce
      should retrieveFrom with a http url separating lines                     ${HttpIOSouceTests.retrieveWithLinesSeparator}
      should retrieveFrom with a http url not separating lines                 ${HttpIOSouceTests.retrieveWithNoRowSeparator}
      should retrieveFrom receiving no data                                    ${HttpIOSouceTests.retrieveWithNoData}
      should retrieveFrom with read timeout                                    ${HttpIOSouceTests.retrieveWithTimeout}
    MultipleHttpIOSource
      should retrieve data from multiple URLs                                  ${MultipleHttpIOSouceTests.retrieveWithLinesSeparatorFromTwoServers}
      should retrieve with read timeout                                        ${MultipleHttpIOSouceTests.retrieveWithTimeout}
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
      val vfsIO = HttpIOSource[PlainTextFormat](HttpIOConfig("http://localhost:8090/test"))
      val result = vfsIO.retrieveInto(intermediate)
      server.stop
      
      (result must beSuccessfulTry) and {
        intermediate.data must be_==(List(List("""{ "test1" : "value1", "test2" : 45 }"""), List("""{ "test1" : "value2", "test2" : 67 }""")))
      }
    }
    def retrieveWithNoRowSeparator = {
      val server = new StubServer(8090).defaultResponse(ContentType("text/plain"), jsonTestDataNoSeparator, 200).start

      val intermediate = new MemoryIntermediate[PlainTextFormat]("memory:test")
      val vfsIO = HttpIOSource[PlainTextFormat](HttpIOConfig("http://localhost:8090/test"), rowSeparator = RowSeparator.NoSeparator)
      val result = vfsIO.retrieveInto(intermediate)
      server.stop
      
      (result must beSuccessfulTry) and {
        intermediate.data must be_==(List(List("""{"test1" : "value1","test2" : 45}""")))
      }
    }
    def retrieveWithNoData = {
      val server = new StubServer(8090).defaultResponse(ContentType("text/plain"), "", 200).start

      val intermediate = new MemoryIntermediate[PlainTextFormat]("memory:test")
      val vfsIO = HttpIOSource[PlainTextFormat](HttpIOConfig("http://localhost:8090/test"), rowSeparator = RowSeparator.NoSeparator)
      val result = vfsIO.retrieveInto(intermediate)
      server.stop
      
      (result must beSuccessfulTry) and {
        intermediate.data must be_==(List())
      }
    }
    def retrieveWithTimeout = {
      val route = GET (
        path = "/test",
        response = DynamicServerResponse { request =>
          Thread.sleep(100)
          StaticServerResponse(ContentType("text/plain"), jsonTestData1, 200)
        }
      )
      val server = new StubServer(8090, route).start

      val intermediate = new MemoryIntermediate[PlainTextFormat]("memory:test")
      val vfsIO = HttpIOSource[PlainTextFormat](HttpIOConfig("http://localhost:8090/test", readTimeout=50))
      val result = vfsIO.retrieveInto(intermediate)
      server.stop
      
      (result must beFailedTry.withThrowable[SocketTimeoutException]) and {
        intermediate.data must be_==(List())
      }
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
        intermediate.getData must be_==(List(List("""{ "test1" : "value1", "test2" : 45 }"""), List("""{ "test1" : "value2", "test2" : 67 }"""), List("""{ "testA" : "valueA", "testB" : 12 }"""), List("""{ "testA" : "valueB", "testB" : 34 }""")))
      }
    }
    def retrieveWithTimeout = {
      val route = GET (
        path = "/test",
        response = DynamicServerResponse { request =>
          Thread.sleep(100)
          StaticServerResponse(ContentType("text/plain"), jsonTestData1, 200)
        }
      )
      val server1 = new StubServer(8090, route).start
      val server2 = new StubServer(8091).defaultResponse(ContentType("text/plain"), jsonTestData2, 200).start

      val intermediate = new MemoryIntermediate[PlainTextFormat]("memory:test")
      val vfsIO = MultipleHttpIOSource[PlainTextFormat](new MultipleHttpIOConfig() {
        def urls = List("http://localhost:8090/test", "http://localhost:8091/test")
        def combinedFileUrl = newTempFileUrl()
        override def readTimeout = 50
      })
      val result = vfsIO.retrieveInto(intermediate)
      server1.stop
      server2.stop
      
      (result must beFailedTry.withThrowable[SocketTimeoutException]) and {
        intermediate.data must be_==(List())
      }
    }
  }
}