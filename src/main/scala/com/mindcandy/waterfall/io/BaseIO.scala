package com.mindcandy.waterfall.io

import com.mindcandy.waterfall.IntermediateFormat
import com.mindcandy.waterfall.Intermediate
import com.mindcandy.waterfall.FileIntermediate
import com.mindcandy.waterfall.S3Intermediate
import com.mindcandy.waterfall.IOConfig
import com.mindcandy.waterfall.IOSource
import com.mindcandy.waterfall.IOSink
import com.typesafe.scalalogging.slf4j.Logging
import java.io.IOException
import java.nio.file.Files
import org.apache.commons.vfs2.VFS
import java.io.BufferedReader
import java.io.InputStreamReader
import resource._
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import com.mindcandy.waterfall.IOOps
import com.github.nscala_time.time.Imports._
import com.mindcandy.waterfall.RowSeparator._
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import com.mindcandy.waterfall.IntermediateOps

case class BaseIOConfig(url: String) extends IOConfig
case class S3IOConfig(url: String, awsAccessKey: String, awsSecretKey: String, bucketName: String, keyPrefix: String,
    keyDate: DateTime = DateTime.now, columnSeparator: Option[String] = Option("\t")) extends IOConfig

case class FileIO[A](config: IOConfig, columnSeparator: Option[String] = Option("\t"))
  extends IOSource[A]
  with IOSink[A] {

  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    // reusing the FileIntermediate for file reading
    val inputFile = FileIntermediate[A](config.url, columnSeparator)
    inputFile.read(intermediate.write).map { _ =>
      logger.info("Retrieving into %s from %s completed".format(intermediate, config))
    }
  }

  def storeFrom[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    // reusing the FileIntermediate for file writing
    val outputFile = FileIntermediate[A](config.url, columnSeparator)
    intermediate.read(outputFile.write).map { _ =>
      logger.info("Store from %s into %s completed".format(intermediate, config))
    }
  }
}

case class S3IO[A](config: S3IOConfig)
  extends IOSource[A]
  with IOSink[A] {

  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    // reusing the S3Intermediate for file reading
    val inputFile = S3Intermediate[A](config.url, config.awsAccessKey, config.awsSecretKey, config.bucketName, config.keyPrefix, config.keyDate, config.columnSeparator)
    inputFile.read(intermediate.write).map { _ =>
      logger.info("Retrieving into %s completed".format(intermediate))
    }
  }

  def storeFrom[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    // reusing the S3Intermediate for file writing
    val outputFile = S3Intermediate[A](config.url, config.awsAccessKey, config.awsSecretKey, config.bucketName, config.keyPrefix)
    intermediate.read(outputFile.write).map { _ =>
      logger.info("Retrieving into %s completed".format(intermediate))
    }
  }
}

case class ApacheVfsIO[A](config: IOConfig, override val columnSeparator: Option[String] = None, val rowSeparator: RowSeparator = NewLine)
  extends IOSource[A]
  with IOSink[A]
  with IOOps[A]
  with IntermediateOps {

  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]): Try[Unit] = {
    val inputContent = fileContent.map { content =>
      for {
        reader <- managed(new BufferedReader(new InputStreamReader(content.getInputStream())))
      } yield {
        val rawData = Iterator.continually {
          Option(reader.readLine())
        }.takeWhile(_.nonEmpty).flatten
      
        rowSeparator match {
          case NewLine => rawData.map { fromLine(_) }
          case NoSeparator => {
            rawData.mkString("") match {
              case combinedData if !combinedData.isEmpty => Iterator(fromLine(combinedData))
              case _ => Iterator[A]()
            }
          }
        }
      }
    }

    inputContent.flatMap { resource => resource.acquireFor(intermediate.write).convertToTry.map { _ =>
      logger.info("Retrieving into %s from %s completed".format(intermediate, config))
    }}
  }

  def storeFrom[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    fileContent.map { content =>
      for {
        writer <- managed(new BufferedWriter(new OutputStreamWriter(content.getOutputStream())))
      } {
        intermediate.read {
          _.foreach { input =>
            writer.write(toLine(input))
            rowSeparator match {
              case NewLine => writer.newLine()
              case NoSeparator =>
            }
          }
        }
      }
    }
  }

  private[this] def fileContent = Try {
    val fileObject = VFS.getManager().resolveFile(config.url);
    fileObject.getContent()
  }
}
