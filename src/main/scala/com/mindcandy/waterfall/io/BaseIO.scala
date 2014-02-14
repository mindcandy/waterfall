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

case class BaseIOConfig(url: String) extends IOConfig
case class S3IOConfig(url: String, awsAccessKey: String, awsSecretKey: String, bucketName: String, keyPrefix: String,
    keyDate: DateTime = DateTime.now, columnSeparator: Option[String] = Option("\t")) extends IOConfig

case class FileIO[A](config: IOConfig, columnSeparator: Option[String] = Option("\t"))
  extends IOSource[A]
  with IOSink[A] {

  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    // reusing the FileIntermediate for file reading
    val inputFile = FileIntermediate[A](config.url, columnSeparator)
    inputFile.read.acquireFor(intermediate.write) match {
      case Left(exceptions) => handleErrors(exceptions)
      case Right(result) => logger.info("Retrieving into %s from %s completed".format(intermediate, config))
    }
  }

  def storeFrom[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    // reusing the FileIntermediate for file writing
    val outputFile = FileIntermediate[A](config.url, columnSeparator)
    intermediate.read.acquireFor(outputFile.write) match {
      case Left(exceptions) => handleErrors(exceptions)
      case Right(result) => logger.info("Store from %s into %s completed".format(intermediate, config))
    }
  }
}

case class S3IO[A](config: S3IOConfig)
  extends IOSource[A]
  with IOSink[A] {

  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    // reusing the S3Intermediate for file reading
    val inputFile = S3Intermediate[A](config.url, config.awsAccessKey, config.awsSecretKey, config.bucketName, config.keyPrefix, config.keyDate, config.columnSeparator)
    inputFile.read.acquireFor(intermediate.write) match {
      case Left(exceptions) => handleErrors(exceptions)
      case Right(result) => logger.info("Retrieving into %s completed".format(intermediate))
    }
  }

  def storeFrom[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    // reusing the S3Intermediate for file writing
    val outputFile = S3Intermediate[A](config.url, config.awsAccessKey, config.awsSecretKey, config.bucketName, config.keyPrefix)
    intermediate.read.acquireFor(outputFile.write) match {
      case Left(exceptions) => handleErrors(exceptions)
      case Right(result) => logger.info("Retrieving into %s completed".format(intermediate))
    }
  }
}

case class ApacheVfsIO[A](config: IOConfig, override val columnSeparator: Option[String] = None, val rowSeparator: RowSeparator = NewLine)
  extends IOSource[A]
  with IOSink[A]
  with IOOps[A] {

  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    val inputContent = for {
      reader <- managed(new BufferedReader(new InputStreamReader(fileContent.getInputStream())))
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

    inputContent.acquireFor(intermediate.write) match {
      case Left(exceptions) => handleErrors(exceptions)
      case Right(result) => logger.info("Retrieving into %s from %s completed".format(intermediate, config))
    }
  }

  def storeFrom[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    for {
      writer <- managed(new BufferedWriter(new OutputStreamWriter(fileContent.getOutputStream())))
    } {
      intermediate.read.acquireFor {
        _.foreach { input =>
          writer.write(toLine(input))
          rowSeparator match {
            case NewLine => writer.newLine()
            case NoSeparator =>
          }
        }
      } match {
        case Left(exceptions) => handleErrors(exceptions)
        case Right(result) => logger.info("Store from %s into %s completed".format(intermediate, config))
      }
    }
  }

  private[this] def fileContent = {
    val fileObject = VFS.getManager().resolveFile(config.url);
    fileObject.getContent()
  }
}