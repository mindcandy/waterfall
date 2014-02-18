package com.mindcandy.waterfall.io

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
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

@RunWith(classOf[JUnitRunner])
class BaseIOSpec extends Specification with Mockito {
  val jsonTestData = """|{ "test1" : "value1", "test2" : 45 }
                        |{ "test1" : "value2", "test2" : 67 }""".stripMargin
                        
  val jsonTestDataNoSeparator = """|{
                                   |"test1" : "value1",
                                   |"test2" : 45
                                   |}""".stripMargin

  case class FailingIntermediate[A](url: String) extends Intermediate[A] {
    val data = Seq[Seq[String]]()

    implicit def SeqResource[B <: Seq[_]] = new Resource[B] {
      override def close(r: B) = ()
    }

    def read(implicit format: IntermediateFormat[A]): ManagedResource[Iterator[A]] = {
      for { reader <- managed(data) } yield {
        throw new Exception("this intermediate will always fail")
      }
    }

    def write(stream: Iterator[A])(implicit format: IntermediateFormat[A]): Unit = {
      throw new Exception("this intermediate will always fail")
    }
  }

  sequential
  
  "FileIO" should {
    "fail storeFrom with an io exception if the underlying intermediate cannot read" in {
      val fileIO = FileIO[TestFormat](BaseIOConfig("/tmp/waterfall-test-file.tsv"))
      fileIO.storeFrom(FailingIntermediate[TestFormat]("nothing")) must throwA[IOException]
    }
  }

  "ApacheVfsIO" should {
    "should retrieveFrom with a http url on vfs" in {
      val server = new StubServer(8080).defaultResponse(ContentType("text/plain"), jsonTestData, 200).start

      val intermediate = new MemoryIntermediate[PlainTextFormat]("memory:test")
      val vfsIO = ApacheVfsIO[PlainTextFormat](BaseIOConfig("http://localhost:8080/test"))
      vfsIO.retrieveInto(intermediate)
      server.stop
      intermediate.data must haveTheSameElementsAs(List(List("""{ "test1" : "value1", "test2" : 45 }"""), List("""{ "test1" : "value2", "test2" : 67 }""")))
    }
    
    "should retrieveFrom with a http url on vfs using no separator" in {
      val server = new StubServer(8080).defaultResponse(ContentType("text/plain"), jsonTestDataNoSeparator, 200).start

      val intermediate = new MemoryIntermediate[PlainTextFormat]("memory:test")
      val vfsIO = ApacheVfsIO[PlainTextFormat](BaseIOConfig("http://localhost:8080/test"), rowSeparator = RowSeparator.NoSeparator)
      vfsIO.retrieveInto(intermediate)
      server.stop
      intermediate.data must haveTheSameElementsAs(List(List("""{"test1" : "value1","test2" : 45}""")))
    }
    
    "should retrieveFrom with a http url on vfs having no data" in {
      val server = new StubServer(8080).defaultResponse(ContentType("text/plain"), "", 200).start

      val intermediate = new MemoryIntermediate[PlainTextFormat]("memory:test")
      val vfsIO = ApacheVfsIO[PlainTextFormat](BaseIOConfig("http://localhost:8080/test"), rowSeparator = RowSeparator.NoSeparator)
      vfsIO.retrieveInto(intermediate)
      server.stop
      intermediate.data must haveTheSameElementsAs(List())
    }

    "should storeFrom with a file url on vfs" in {
      val testFile = Files.createTempFile("test-waterfall-", ".txt")

      val intermediate = new MemoryIntermediate[PlainTextFormat]("memory:test")
      intermediate.data ++= (List(List("""{ "test1" : "value1", "test2" : 45 }"""), List("""{ "test1" : "value2", "test2" : 67 }""")))

      val vfsIO = ApacheVfsIO[PlainTextFormat](BaseIOConfig(testFile.toUri.toString))
      vfsIO.storeFrom(intermediate)

      Files.readAllLines(testFile, Charset.defaultCharset).asScala must haveTheSameElementsAs(List("""{ "test1" : "value1", "test2" : 45 }""",
          """{ "test1" : "value2", "test2" : 67 }"""))
    }
    
    "should storeFrom with a file url on vfs with no separator" in {
      val testFile = Files.createTempFile("test-waterfall-", ".txt")

      val intermediate = new MemoryIntermediate[PlainTextFormat]("memory:test")
      intermediate.data ++= (List(List("""{ "test1" : "value1", "test2" : 45 }"""), List("""{ "test1" : "value2", "test2" : 67 }""")))

      val vfsIO = ApacheVfsIO[PlainTextFormat](BaseIOConfig(testFile.toUri.toString), rowSeparator = RowSeparator.NoSeparator)
      vfsIO.storeFrom(intermediate)

      Files.readAllLines(testFile, Charset.defaultCharset).asScala must haveTheSameElementsAs(List("""{ "test1" : "value1", "test2" : 45 }{ "test1" : "value2", "test2" : 67 }"""))
    }
  }

  "HttpIOSource" should {
    "should retrieveFrom with a http url" in {
      val server = new StubServer(8080).defaultResponse(ContentType("text/plain"), jsonTestData, 200).start

      val intermediate = new MemoryIntermediate[PlainTextFormat]("memory:test")
      val vfsIO = HttpIOSource[PlainTextFormat](BaseIOConfig("http://localhost:8080/test"))
      vfsIO.retrieveInto(intermediate)
      server.stop
      intermediate.data must haveTheSameElementsAs(List(List("""{ "test1" : "value1", "test2" : 45 }"""), List("""{ "test1" : "value2", "test2" : 67 }""")))
    }

    "should retrieveFrom with a http url using no separator" in {
      val server = new StubServer(8080).defaultResponse(ContentType("text/plain"), jsonTestDataNoSeparator, 200).start

      val intermediate = new MemoryIntermediate[PlainTextFormat]("memory:test")
      val vfsIO = HttpIOSource[PlainTextFormat](BaseIOConfig("http://localhost:8080/test"), rowSeparator = RowSeparator.NoSeparator)
      vfsIO.retrieveInto(intermediate)
      server.stop
      intermediate.data must haveTheSameElementsAs(List(List("""{"test1" : "value1","test2" : 45}""")))
    }

    "should retrieveFrom with a http url having no data" in {
      val server = new StubServer(8080).defaultResponse(ContentType("text/plain"), "", 200).start

      val intermediate = new MemoryIntermediate[PlainTextFormat]("memory:test")
      val vfsIO = HttpIOSource[PlainTextFormat](BaseIOConfig("http://localhost:8080/test"), rowSeparator = RowSeparator.NoSeparator)
      vfsIO.retrieveInto(intermediate)
      server.stop
      intermediate.data must haveTheSameElementsAs(List())
    }
  }
}