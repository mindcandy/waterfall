package com.mindcandy.waterfall

trait IntermediateFormatCompanion[A] {
  implicit def format: IntermediateFormat[A]
}

trait IntermediateFormat[A] {
  def convertTo(value: Seq[String]): A
  def convertFrom(value: A): Seq[String]
}

case class PlainTextFormat(line: String)

object PlainTextFormat extends IntermediateFormatCompanion[PlainTextFormat] {
  object format extends IntermediateFormat[PlainTextFormat] {
    def convertTo(input: Seq[String]) = PlainTextFormat(input(0))
    def convertFrom(input: PlainTextFormat) = Seq[String](input.line)
  }
}
