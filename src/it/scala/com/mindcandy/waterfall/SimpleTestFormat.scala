package com.mindcandy.waterfall

case class SimpleTestFormat(id: Int, name: String)

object SimpleTestFormat extends IntermediateFormatCompanion[SimpleTestFormat] {
  object format extends IntermediateFormat[SimpleTestFormat] {
    def convertTo(input: Seq[String]) = SimpleTestFormat(input(0).toInt, input(1))
    def convertFrom(input: SimpleTestFormat) = Seq[String](input.id.toString, input.name)
  }
}