package com.mindcandy.waterfall

import com.typesafe.scalalogging.slf4j.Logging
import java.io.IOException
import java.nio.file.Files

trait IOConfig {
  def url: String
}

trait IOOps[A] {
  val columnSeparator = "\t"
  def toLine(input: A)(implicit format: IntermediateFormat[A]) = {
    val rawInput = format.convertFrom(input)
    rawInput.tail.foldLeft(rawInput.head)("%s%s%s".format(_, columnSeparator, _))
  }
  def newTempFileUrl() = {
    val file = Files.createTempFile("waterfall-", ".tsv")
    file.toFile.deleteOnExit()
    file.toUri.toString
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