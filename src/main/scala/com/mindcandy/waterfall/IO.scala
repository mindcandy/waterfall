package com.mindcandy.waterfall

import com.typesafe.scalalogging.slf4j.Logging
import java.io.IOException
import java.nio.file.Files
import scala.util.Try

trait IOConfig {
  def url: String
}

trait IOOps[A] {
  val columnSeparator: Option[String]
  def fromLine(input: String)(implicit format: IntermediateFormat[A]) = {
    columnSeparator match {
      case Some(separator) => format.convertTo(input.split(separator))
      case None => format.convertTo(Seq(input))
    }
  }
  def toLine(input: A)(implicit format: IntermediateFormat[A]) = {
    val rawInput = format.convertFrom(input)
    rawInput.tail.foldLeft(rawInput.head) {
      columnSeparator match {
        case Some(separator) => "%s%s%s".format(_, separator, _)
        case None => "%s%s".format(_, _)
      }
    }
  }
}

trait IOBase extends Logging {
  def config: IOConfig
}

trait IOSource[A] extends IOBase {
  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]): Try[Unit]
}

trait IOSink[A] extends IOBase {
  def storeFrom[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]): Try[Unit]
}

object RowSeparator extends Enumeration {
  type RowSeparator = Value
  val NewLine, NoSeparator = Value
}