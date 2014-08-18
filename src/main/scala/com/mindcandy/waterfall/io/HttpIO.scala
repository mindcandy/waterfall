package com.mindcandy.waterfall.io

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.util.Timeout
import com.mindcandy.waterfall.{ IOConfig, IOOps, IOSource, Intermediate, IntermediateFormat }
import com.mindcandy.waterfall.RowSeparator._
import com.mindcandy.waterfall.intermediate.FileIntermediate
import com.typesafe.scalalogging.slf4j.Logging
import spray.client.pipelining._
import spray.http._

import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.util.Try

case class HttpIOConfig(url: String, timeout: Int = 5000) extends IOConfig

case class HttpIOSource[A <: AnyRef](config: HttpIOConfig, override val columnSeparator: Option[String] = None, val rowSeparator: RowSeparator = NewLine)
    extends IOSource[A]
    with IOOps[A] {

  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    val inputContent = rowSeparator match {
      case NewLine => fileContent.map {
        _.lines.map(fromLine(_))
      }
      case NoSeparator => {
        fileContent.map {
          _.lines.mkString("") match {
            case combinedData if !combinedData.isEmpty => Iterator(fromLine(combinedData))
            case _ => Iterator[A]()
          }
        }
      }
    }
    system.shutdown()
    system.awaitTermination()

    inputContent.flatMap { content =>
      intermediate.write(content).map { _ =>
        logger.info("Retrieving into %s from %s completed".format(intermediate, config))
      }
    }
  }

  implicit val system = ActorSystem("waterfall-httpio")
  implicit val executionContext: ExecutionContext = system.dispatcher
  implicit val timeout = Timeout(config.timeout, TimeUnit.MILLISECONDS)
  val pipeline: HttpRequest => Future[String] = sendReceive ~> unmarshal[String]

  private[this] def fileContent: Try[String] = Try {
    Await.result(pipeline(Get(config.url)), Duration.Inf)
  }
}

trait MultipleHttpIOConfig extends IOConfig {
  def urls: List[String]
  def combinedFileUrl: String
  def timeout: Int = 5000
  override def url = urls.mkString(";")
  override def toString = "MultipleHttpIOConfig(%s)".format(urls)
}

case class MultipleHttpIOSource[A <: AnyRef](config: MultipleHttpIOConfig) extends IOSource[A] with Logging {
  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    val combinedIntermediate = FileIntermediate[A](config.combinedFileUrl)
    val result = generateHttpIOConfigs(config).foldLeft(Try(())) { (previousResult, httpIOConfig) =>
      previousResult.flatMap { _ =>
        HttpIOSource[A](httpIOConfig).retrieveInto(combinedIntermediate)(format)
      }
    }
    result.flatMap { _ =>
      combinedIntermediate.read(intermediate.write)(format).map { _ =>
        logger.info("Retrieving into %s from %s completed".format(intermediate, config))
      }
    }
  }

  def generateHttpIOConfigs(config: MultipleHttpIOConfig) = {
    config.urls.map { url => HttpIOConfig(url, config.timeout) }
  }
}