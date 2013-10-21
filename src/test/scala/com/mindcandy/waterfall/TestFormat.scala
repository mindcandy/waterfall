package com.mindcandy.waterfall

import com.github.nscala_time.time.Imports._

case class TestFormat(id: Int, name: String, added: DateTime)

object TestFormat extends IntermediateFormatCompanion[TestFormat] {
  object format extends IntermediateFormat[TestFormat] {
    def convertTo(input: Seq[String]) = TestFormat(input(0).toInt, input(1), DateTime.now)
    def convertFrom(input: TestFormat) = Seq[String](input.id.toString, input.name, input.added.toString)
  }
}