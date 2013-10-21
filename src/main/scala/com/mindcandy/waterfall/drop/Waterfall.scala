package com.mindcandy.waterfall.drop
import com.mindcandy.waterfall.IntermediateFormat
import scala.slick.driver.PostgresDriver.simple._
import com.mindcandy.waterfall.IntermediateFormatCompanion
import com.mindcandy.waterfall.MemoryIntermediate
import com.mindcandy.waterfall.io.SqlIOSource
import com.mindcandy.waterfall.FileIntermediate
import com.mindcandy.waterfall.io.SqlIOConfig

case class TestFormat(id: Int, name: String)

object TestFormat extends IntermediateFormatCompanion[TestFormat] {
  object format extends IntermediateFormat[TestFormat] {
    def convertTo(input: Seq[String]) = TestFormat(input(0).toInt, input(1))
    def convertFrom(input: TestFormat) = Seq[String](input.id.toString, input.name)
  }
}

object Waterfall extends App {
  println("Waterfall running")
  
  val test = MemoryIntermediate[TestFormat]("mem:test")
  val source = SqlIOSource[TestFormat](SqlIOConfig("jdbc:postgresql:waterfall", "org.postgresql.Driver", "kevin.schmidt", "", "select * from test_table"))
  source.retrieveInto(test)
  
  val man = test.read
  val res = man.acquireFor(_.foreach(println))
  
  res match {
    case Left(exception) => exception.foreach(println)
    case Right(result) => println("Works!")
  }
  
  val testfile = FileIntermediate[TestFormat]("file:///tmp/test1.tsv")
  source.retrieveInto(testfile)
  
  testfile.read.acquireFor(_.foreach(println))
}

