package com.mindcandy.waterfall

import com.typesafe.scalalogging.slf4j.Logging
import java.io.IOException
import java.nio.file.Files

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
  def handleErrors(exceptions: List[Throwable]) = {
    exceptions match {
      case Nil => throw new IOException("Unknown error during IO operation")
      case (head :: tail) => { 
        tail.foreach(logger.error("Exception during IO operation", _))
        throw new IOException("Exception during IO operation", head)
      }
    }
  }
}

trait IOSource[A] extends IOBase {
  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]): Unit
}

trait IOSink[A] extends IOBase {
  def storeFrom[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]): Unit
}