package com.mindcandy.waterfall

import resource._
import com.github.nscala_time.time.Imports._
import java.nio.file.Files
import java.nio.charset.Charset
import java.nio.file.Paths
import java.net.URI
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.BasicAWSCredentials
import java.nio.file.Path
import com.amazonaws.services.s3.model.ObjectMetadata
import java.io.File
import com.typesafe.scalalogging.slf4j.Logging
import scala.annotation.tailrec
import java.nio.file.FileSystems

trait Intermediate[A] {
  def url: String
  def read(implicit format: IntermediateFormat[A]): ManagedResource[Iterator[A]]
  def write(stream: Iterator[A])(implicit format: IntermediateFormat[A]): Unit
}

case class MemoryIntermediate[A](url: String) extends Intermediate[A] {
  val data = collection.mutable.ArrayBuffer[Seq[String]]()

  implicit def SeqResource[B <: Seq[_]] = new Resource[B] {
    override def close(r: B) = ()
  }

  def read(implicit format: IntermediateFormat[A]): ManagedResource[Iterator[A]] = {
    for {
      reader <- managed(data)
    } yield {
      reader.map(format.convertTo).iterator
    }
  }
  def write(stream: Iterator[A])(implicit format: IntermediateFormat[A]): Unit = {
    data.clear()
    data ++= stream.map(format.convertFrom)
  }
}

case class FileIntermediate[A](url: String, override val columnSeparator: Option[String] = Option("\t")) extends Intermediate[A] with IOOps[A] {
  def read(implicit format: IntermediateFormat[A]): ManagedResource[Iterator[A]] = {
    val path = Paths.get(new URI(url))
    for {
      reader <- managed(Files.newBufferedReader(path, Charset.defaultCharset()))
    } yield {
      Iterator.continually {
        Option(reader.readLine())
      }.takeWhile(_.nonEmpty).map { line =>
        fromLine(line.get)
      }
    }
  }

  def write(stream: Iterator[A])(implicit format: IntermediateFormat[A]): Unit = {
    val path = Paths.get(new URI(url))
    for {
      writer <- managed(Files.newBufferedWriter(path, Charset.defaultCharset()))
    } {
      stream.foreach { input =>
        writer.write(toLine(input))
        writer.newLine()
      }
    }
  }
}

case class S3Intermediate[A](url: String, awsAccessKey: String, awsSecretKey: String, bucketName: String, keyPrefix: String,
    keyDate: DateTime = DateTime.now, override val columnSeparator: Option[String] = Option("\t"))
  extends Intermediate[A]
  with IOOps[A]
  with Logging {

  val fileChunkSize = 100 * 1024 * 1024 // 100MB
  val dateFormat = DateTimeFormat.forPattern("yyyyMMdd");
  val datedKeyPrefix = s"${keyPrefix}-${keyDate.toString(dateFormat)}"

  def read(implicit format: IntermediateFormat[A]): ManagedResource[Iterator[A]] = {
    val path = Paths.get(new URI(url))
    for {
      reader <- managed(Files.newBufferedReader(path, Charset.defaultCharset()))
    } yield {
      Iterator.continually {
        Option(reader.readLine())
      }.takeWhile(_.nonEmpty).map { line =>
        fromLine(line.get)
      }
    }
  }

  def write(stream: Iterator[A])(implicit format: IntermediateFormat[A]): Unit = {
    logger.info(s"Starting upload to S3 with endpoint ${url}, starting write with file chunk size ${fileChunkSize}")
    val numFiles = writeChunkToS3(stream, 0)
    logger.info(s"Upload to S3 completed, ${numFiles} files written with file chunk size ${fileChunkSize}")
  }

  val amazonS3Client = {
    val awsCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey)
    val s3Client = new AmazonS3Client(awsCredentials)
    s3Client.setEndpoint(url)
    s3Client
  }

  @tailrec
  private[this] def writeChunkToS3(stream: Iterator[A], counter: Int)(implicit format: IntermediateFormat[A]): Int = {
    if (stream.hasNext) {
      val uploadFile = Files.createTempFile("waterfall-", "-" + counter + ".tsv")
      var byteCounter = 0
      for {
        writer <- managed(Files.newBufferedWriter(uploadFile, Charset.defaultCharset()))
      } {
        while(stream.hasNext && byteCounter < fileChunkSize) {
          val line = toLine(stream.next)
          writer.write(line)
          writer.newLine()
          byteCounter += line.getBytes("UTF-8").length + FileSystems.getDefault.getSeparator.length
        }
      }

      logger.info(s"Finished writing ${byteCounter} bytes to temporary file ${uploadFile}")
      val keyName = s"${datedKeyPrefix}-${counter}.tsv"
      logger.info(s"Starting S3 upload to bucket/key: ${bucketName}/${keyName}")
      amazonS3Client.putObject(bucketName, keyName, uploadFile.toFile)

      writeChunkToS3(stream, counter + 1)
    } else {
      counter
    }
  }
}