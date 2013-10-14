package com.mindcandy.waterfall

import com.mindcandy.waterfall.io.SqlSource
import com.mindcandy.waterfall.io.SqlIOConfig
import com.mindcandy.waterfall.io.IntermediateFormat
import scala.language.implicitConversions
import scala.slick.driver.PostgresDriver.simple._
import com.mindcandy.waterfall.io.MemoryIntermediate

case class TestFormat(id: Int, name: String)

object TestFormat extends IntermediateFormat[TestFormat] {
  def toConverter(input: Seq[String]) = TestFormat(input(0).toInt, input(1))
  def fromConverter(input: TestFormat) = Seq[String](input.id.toString, input.name)
}

object Waterfall extends App {
  println("Waterfall running")
  
  val test = MemoryIntermediate[TestFormat]("mem:test")
  val source = SqlSource[TestFormat](SqlIOConfig("jdbc:postgresql:waterfall", "org.postgresql.Driver", "kevin.schmidt", "", "select * from test_table"))
  source.retrieveInto(test)
  
  println(test.read.toList)
}

