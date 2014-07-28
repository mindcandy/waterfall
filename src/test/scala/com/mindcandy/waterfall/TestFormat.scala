package com.mindcandy.waterfall

import org.joda.time.DateTime

/**
 * Testing format, read/write 3 params from/into Seq[String]
 * @param id Int, the row id
 * @param name String, the name
 * @param added DateTime, the time when the row is added
 */
case class TestFormat(id: Int, name: String, added: DateTime)

object TestFormat extends IntermediateFormatCompanion[TestFormat] {

  object format extends IntermediateFormat[TestFormat] {

    def convertTo(input: Seq[String]) =
      TestFormat(input(0).toInt, input(1), DateTime.parse(input(2)))

    def convertFrom(input: TestFormat) =
      Seq[String](input.id.toString, input.name, input.added.toString)
  }
}