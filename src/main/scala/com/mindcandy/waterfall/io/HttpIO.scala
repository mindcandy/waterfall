package com.mindcandy.waterfall.io

import com.mindcandy.waterfall.IntermediateFormat
import com.mindcandy.waterfall.Intermediate
import com.mindcandy.waterfall.IOConfig
import com.mindcandy.waterfall.IOSource
import com.typesafe.scalalogging.slf4j.Logging
import com.mindcandy.waterfall.IOOps
import com.mindcandy.waterfall.RowSeparator._
import uk.co.bigbeeconsultants.http._
import scala.util.Try
import com.mindcandy.waterfall.intermediate.FileIntermediate

case class HttpIOConfig(url: String, connectTimeout: Int = 2000, readTimeout: Int = 5000) extends IOConfig

case class HttpIOSource[A](config: HttpIOConfig, override val columnSeparator: Option[String] = None, val rowSeparator: RowSeparator = NewLine)
    extends IOSource[A]
    with IOOps[A] {

  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    val inputContent = Try {
      rowSeparator match {
        case NewLine => fileContent.lines.map { fromLine(_) }
        case NoSeparator => {
          fileContent.lines.mkString("") match {
            case combinedData if !combinedData.isEmpty => Iterator(fromLine(combinedData))
            case _ => Iterator[A]()
          }
        }
      }
    }

    inputContent.map { content =>
      intermediate.write(content).map { _ =>
        logger.info("Retrieving into %s from %s completed".format(intermediate, config))
      }
    }
  }

  private[this] def fileContent = {
    val httpConfig = Config(connectTimeout = config.connectTimeout, readTimeout = config.readTimeout)
    val httpClient = new HttpClient(httpConfig)
    val response = httpClient.get(config.url)
    response.body.asString
  }
}

trait MultipleHttpIOConfig extends IOConfig {
  def urls: List[String]
  def combinedFileUrl: String
  def connectTimeout: Int = 2000
  def readTimeout: Int = 5000
  override def url = urls.mkString(";")
  override def toString = "MultipleHttpIOConfig(%s)".format(urls)
}

case class MultipleHttpIOSource[A](config: MultipleHttpIOConfig) extends IOSource[A] with Logging {
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
    config.urls.map { url => HttpIOConfig(url, config.connectTimeout, config.readTimeout) }
  }
}