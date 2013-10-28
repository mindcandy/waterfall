package com.mindcandy.waterfall

import com.typesafe.scalalogging.slf4j.Logging
import java.io.IOException

trait IOConfig {
  def url: String
}

trait IOOps[A] {
  def columnSeparator = "\t"
  def toLine(input: A)(implicit format: IntermediateFormat[A]) = {
    val rawInput = format.convertFrom(input)
    rawInput.tail.foldLeft(rawInput.head)("%s%s%s".format(_, columnSeparator, _))
  }
}

trait IOBase extends Logging {
  def config: IOConfig
  def handleErrors(exceptions: List[Throwable]) = {
    exceptions.foreach(logger.error("Exception during IO operation", _))
    throw new IOException("Exception during IO operation", exceptions(0))
  }
}

trait IOSource[A] extends IOBase {
  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]): Unit
}

trait IOSink[A] extends IOBase {
  def storeFrom[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]): Unit
}