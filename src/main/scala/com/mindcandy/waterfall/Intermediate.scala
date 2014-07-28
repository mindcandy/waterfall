package com.mindcandy.waterfall

import com.typesafe.scalalogging.slf4j.Logging
import scala.util.Try
import scala.util.Success
import scala.util.Failure

trait Intermediate[A <: AnyRef] extends Logging {
  def url: String
  def read[B](f: Iterator[A] => Try[B])(implicit format: IntermediateFormat[A]): Try[B]
  def write(stream: Iterator[A])(implicit format: IntermediateFormat[A]): Try[Unit]
}

trait IntermediateOps {
  implicit class ManagedResourceOps[A](either: Either[List[Throwable], Try[A]]) extends Logging {
    def convertToTry: Try[A] = either match {
      case Left(exceptions) => exceptions match {
        case Nil => Failure(new Exception("managed resource failure without exception"))
        case head :: tail => {
          tail.foreach(logger.error("Exception during IO operation", _))
          Failure(head)
        }
      }
      case Right(result) => result
    }
  }
}