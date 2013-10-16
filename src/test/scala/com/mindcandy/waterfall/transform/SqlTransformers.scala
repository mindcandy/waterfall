package com.mindcandy.waterfall.transform

import org.specs2.mutable._
import com.mindcandy.waterfall.io.SqlIOConfig
import com.mindcandy.waterfall.io.FileIOConfig
import com.mindcandy.waterfall.IntermediateFormatCompanion
import com.mindcandy.waterfall.IntermediateFormat
import com.github.nscala_time.time.Imports._

class SqlTransformersSpec extends Specification {
  case class Format(id: Int, name: String, added: DateTime)

  object Format extends IntermediateFormatCompanion[Format] {
	object format extends IntermediateFormat[Format] {
	  def convertTo(input: Seq[String]) = Format(input(0).toInt, input(1), DateTime.now)
	  def convertFrom(input: Format) = Seq[String](input.id.toString, input.name, input.added.toString)
	}
  }
  

  "SqlToFile" should {
    "work" in {
      SqlToFile[Format](
        SqlIOConfig("jdbc:postgresql:waterfall", "org.postgresql.Driver", "kevin.schmidt", "", "select * from test_table"),
        FileIOConfig("file:///tmp/test.tsv")
      ).run
      done
    }
  }
}