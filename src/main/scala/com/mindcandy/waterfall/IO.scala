package com.mindcandy.waterfall

import com.typesafe.scalalogging.slf4j.Logging
import scala.util.Try

trait IOConfig {
  def url: String
}

object RowSeparator extends Enumeration {
  type RowSeparator = Value
  val NewLine, NoSeparator = Value
}

trait IOOps[A] {
  import RowSeparator._

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

  def processRowSeparator(rawData: Iterator[String], rowSeparator: RowSeparator)(implicit format: IntermediateFormat[A]) = rowSeparator match {
    case NewLine => rawData.map { fromLine }
    case NoSeparator => {
      rawData.mkString("") match {
        case combinedData if !combinedData.isEmpty => Iterator(fromLine(combinedData))
        case _ => Iterator[A]()
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