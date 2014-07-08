package com.mindcandy.waterfall.io

import com.mindcandy.waterfall.IntermediateFormat
import com.mindcandy.waterfall.Intermediate
import com.mindcandy.waterfall.IOConfig
import com.mindcandy.waterfall.IOSource
import com.mindcandy.waterfall.IOSink
import org.apache.commons.vfs2.VFS
import java.io.BufferedReader
import java.io.InputStreamReader
import resource._
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import com.mindcandy.waterfall.IOOps
import com.mindcandy.waterfall.RowSeparator._
import scala.util.Try
import com.mindcandy.waterfall.IntermediateOps
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.mindcandy.waterfall.intermediate.{ FileIntermediate, MemoryIntermediate }

case class BaseIOConfig(url: String) extends IOConfig
case class S3IOConfig(url: String, awsAccessKey: String, awsSecretKey: String, bucketName: String, key: String) extends IOConfig

case class MemoryIO[A <: AnyRef](config: IOConfig)
    extends IOSource[A]
    with IOSink[A] {

  val memoryIntermediate = MemoryIntermediate[A](config.url)

  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    // reusing the MemoryIntermediate
    memoryIntermediate.read(intermediate.write).map { _ =>
      logger.info("Retrieving into %s from %s completed".format(intermediate, config))
    }
  }

  def storeFrom[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    // reusing the MemoryIntermediate
    intermediate.read(memoryIntermediate.write).map { _ =>
      logger.info("Store from %s into %s completed".format(intermediate, config))
    }
  }
}

case class FileIO[A <: AnyRef](config: IOConfig, columnSeparator: Option[String] = Option("\t"))
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

case class S3IO[A <: AnyRef](config: S3IOConfig, val keySuffix: Option[String] = None, val columnSeparator: Option[String] = Option("\t"), val rowSeparator: RowSeparator = NewLine)
    extends IOSource[A]
    with IOOps[A]
    with IntermediateOps {

  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    val bufferedReader = Try(new BufferedReader(new InputStreamReader(amazonS3Client.getObject(config.bucketName, config.key + keySuffix.getOrElse("")).getObjectContent())))
    val inputContent = bufferedReader.map { bufReader =>
      for {
        reader <- managed(bufReader)
      } yield {
        val rawData = Iterator.continually {
          Option(reader.readLine())
        }.takeWhile(_.nonEmpty).flatten
        processRowSeparator(rawData, rowSeparator)
      }
    }

    inputContent.flatMap { resource =>
      resource.acquireFor(intermediate.write).convertToTry.map { _ =>
        logger.info("Retrieving into %s from %s completed".format(intermediate, config))
      }
    }
  }

  val amazonS3Client = {
    val awsCredentials = new BasicAWSCredentials(config.awsAccessKey, config.awsSecretKey)
    val s3Client = new AmazonS3Client(awsCredentials)
    s3Client.setEndpoint(config.url)
    s3Client
  }
}

case class ApacheVfsIO[A <: AnyRef](config: IOConfig, val columnSeparator: Option[String] = None, val rowSeparator: RowSeparator = NewLine)
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
        processRowSeparator(rawData, rowSeparator)
      }
    }

    inputContent.flatMap { resource =>
      resource.acquireFor(intermediate.write).convertToTry.map { _ =>
        logger.info("Retrieving into %s from %s completed".format(intermediate, config))
      }
    }
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
