package com.mindcandy.waterfall.io

import java.io.IOException
import java.nio.charset.Charset
import java.nio.file.Files

import com.mindcandy.waterfall.{ Intermediate, IntermediateFormat, PlainTextFormat, RowSeparator, TestFormat }
import com.mindcandy.waterfall.intermediate.MemoryIntermediate
import fr.simply.StubServer
import fr.simply.util.ContentType
import org.junit.runner.RunWith
import org.specs2.Specification
import org.specs2.mock.Mockito
import org.specs2.runner.JUnitRunner
import resource._

import scala.collection.JavaConverters._
import scala.util.{ Failure, Try }

@RunWith(classOf[JUnitRunner])
class BaseIOSpec extends Specification with Mockito {
  def is = args.execute(sequential = true) ^ args.report(showtimes = true) ^ s2"""
    FileIO
      fail readInto with an io exception if the file to read is not found                  ${FileIOTests.failReadFileNotFound}
      fail storeFrom with an io exception if the underlying intermediate cannot read       ${FileIOTests.failWriteBadIntermediate}
    ApacheVfsIO
      should retrieveFrom with a http url on vfs                                           ${ApacheVfsIOTests.retrieveWithLineSeparator}
      should retrieveFrom with a http url on vfs using no separator                        ${ApacheVfsIOTests.retrieveWithNoSeparator}
      should retrieveFrom with a http url on vfs having no data                            ${ApacheVfsIOTests.retrieveWithNoData}
      should storeFrom with a file url on vfs                                              ${ApacheVfsIOTests.storeToFileWithLineSeparator}
      should storeFrom with a file url on vfs with no separator                            ${ApacheVfsIOTests.storeToFileWithNoSeparator}
  """

  val jsonTestData1 = """|{ "test1" : "value1", "test2" : 45 }
                        |{ "test1" : "value2", "test2" : 67 }""".stripMargin

  val jsonTestData2 = """|{ "testA" : "valueA", "testB" : 12 }
                        |{ "testA" : "valueB", "testB" : 34 }""".stripMargin

  val jsonTestDataNoSeparator = """|{
                                   |"test1" : "value1",
                                   |"test2" : 45
                                   |}""".stripMargin

  case class FailingIntermediate[A <: AnyRef](url: String) extends Intermediate[A] {
    val data = Seq[Seq[String]]()

    implicit def SeqResource[B <: Seq[_]] = new Resource[B] {
      override def close(r: B) = ()
    }

    def read[B](f: Iterator[A] => Try[B])(implicit format: IntermediateFormat[A]): Try[B] = {
      Failure(new IOException("this intermediate will always fail"))
    }

    def write(stream: Iterator[A])(implicit format: IntermediateFormat[A]): Try[Unit] = {
      Failure(new IOException("this intermediate will always fail"))
    }
  }

  object FileIOTests {
    def failReadFileNotFound = {
      val fileIO = FileIO[TestFormat](BaseIOConfig("file:///tmp/waterfall-test-file-does-not-exists.tsv"))
      val result = fileIO.retrieveInto(FailingIntermediate[TestFormat]("nothing"))

      result must beFailedTry.withThrowable[IOException]
    }

    def failWriteBadIntermediate = {
      val fileIO = FileIO[TestFormat](BaseIOConfig("file:///tmp/waterfall-test-file.tsv"))
      val result = fileIO.storeFrom(FailingIntermediate[TestFormat]("nothing"))

      result must beFailedTry.withThrowable[IOException]
    }
  }

  object ApacheVfsIOTests {
    def retrieveWithLineSeparator = {
      val server = new StubServer(8080).defaultResponse(ContentType("text/plain"), jsonTestData1, 200).start

      val intermediate = new MemoryIntermediate[PlainTextFormat]("memory:test")
      val vfsIO = ApacheVfsIO[PlainTextFormat](BaseIOConfig("http://localhost:8080/test"))
      val result = vfsIO.retrieveInto(intermediate)
      server.stop

      (result must beSuccessfulTry) and {
        intermediate.getData must be_==(List(List("""{ "test1" : "value1", "test2" : 45 }"""), List("""{ "test1" : "value2", "test2" : 67 }""")))
      }
    }

    def retrieveWithNoSeparator = {
      val server = new StubServer(8080).defaultResponse(ContentType("text/plain"), jsonTestDataNoSeparator, 200).start

      val intermediate = new MemoryIntermediate[PlainTextFormat]("memory:test")
      val vfsIO = ApacheVfsIO[PlainTextFormat](BaseIOConfig("http://localhost:8080/test"), rowSeparator = RowSeparator.NoSeparator)
      val result = vfsIO.retrieveInto(intermediate)
      server.stop

      (result must beSuccessfulTry) and {
        intermediate.getData must be_==(List(List("""{"test1" : "value1","test2" : 45}""")))
      }
    }

    def retrieveWithNoData = {
      val server = new StubServer(8080).defaultResponse(ContentType("text/plain"), "", 200).start

      val intermediate = new MemoryIntermediate[PlainTextFormat]("memory:test")
      val vfsIO = ApacheVfsIO[PlainTextFormat](BaseIOConfig("http://localhost:8080/test"), rowSeparator = RowSeparator.NoSeparator)
      val result = vfsIO.retrieveInto(intermediate)
      server.stop

      (result must beSuccessfulTry) and {
        intermediate.getData must be_==(List())
      }
    }

    def storeToFileWithLineSeparator = {
      val testFile = Files.createTempFile("test-waterfall-", ".txt")

      val intermediate = new MemoryIntermediate[PlainTextFormat]("memory:test")
      intermediate.data ++= (List(List("""{ "test1" : "value1", "test2" : 45 }"""), List("""{ "test1" : "value2", "test2" : 67 }""")))

      val vfsIO = ApacheVfsIO[PlainTextFormat](BaseIOConfig(testFile.toUri.toString))
      val result = vfsIO.storeFrom(intermediate)

      (result must beSuccessfulTry) and {
        Files.readAllLines(testFile, Charset.defaultCharset).asScala must be_==(List("""{ "test1" : "value1", "test2" : 45 }""",
          """{ "test1" : "value2", "test2" : 67 }"""))
      }
    }

    def storeToFileWithNoSeparator = {
      val testFile = Files.createTempFile("test-waterfall-", ".txt")

      val intermediate = new MemoryIntermediate[PlainTextFormat]("memory:test")
      intermediate.data ++= (List(List("""{ "test1" : "value1", "test2" : 45 }"""), List("""{ "test1" : "value2", "test2" : 67 }""")))

      val vfsIO = ApacheVfsIO[PlainTextFormat](BaseIOConfig(testFile.toUri.toString), rowSeparator = RowSeparator.NoSeparator)
      val result = vfsIO.storeFrom(intermediate)

      (result must beSuccessfulTry) and {
        Files.readAllLines(testFile, Charset.defaultCharset).asScala must be_==(List("""{ "test1" : "value1", "test2" : 45 }{ "test1" : "value2", "test2" : 67 }"""))
      }
    }
  }
}