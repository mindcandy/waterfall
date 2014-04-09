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
import java.nio.file.StandardOpenOption
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import scala.collection.JavaConverters._
import com.amazonaws.services.s3.model.GetObjectRequest

trait Intermediate[A] extends Logging {
  def url: String
  def read[B](f: Iterator[A] => B)(implicit format: IntermediateFormat[A]): Try[B]
  def write(stream: Iterator[A])(implicit format: IntermediateFormat[A]): Try[Unit]
}

trait IntermediateOps {
  implicit class ManagedResourceOps[A](either: Either[List[Throwable], A]) extends Logging {
    def convertToTry: Try[A] = either match {
      case Left(exceptions) => exceptions match {
        case Nil => Failure(new Exception("managed resource failure without exception"))
        case head :: tail => {
          tail.foreach(logger.error("Exception during IO operation", _))
          Failure(head)
        }
      }
      case Right(result) => Success(result)
    }
  }
}

case class MemoryIntermediate[A](url: String) extends Intermediate[A] {
  val data = collection.mutable.ArrayBuffer[Seq[String]]()

  def read[B](f: Iterator[A] => B)(implicit format: IntermediateFormat[A]): Try[B] = {
    Try(f(data.map(format.convertTo).iterator))
  }
  def write(stream: Iterator[A])(implicit format: IntermediateFormat[A]): Try[Unit] = {
    data ++= stream.map(format.convertFrom)
    Try(())
  }
  def getData(): List[Seq[String]] = {
    data.toList
  }
  def clearData() {
    data.clear()
  }
}

case class FileIntermediate[A](url: String, override val columnSeparator: Option[String] = Option("\t")) extends Intermediate[A] with IOOps[A] with IntermediateOps {

  def read[B](f: Iterator[A] => B)(implicit format: IntermediateFormat[A]): Try[B] = {
    val path = Paths.get(new URI(url))
    val bufferedReader = Try(Files.newBufferedReader(path, Charset.defaultCharset()))
    val managedResource = bufferedReader.map { bufReader =>
      for {
        reader <- managed(bufReader)
      } yield {
        Iterator.continually {
          Option(reader.readLine())
        }.takeWhile(_.nonEmpty).map { line =>
          fromLine(line.get)
        }
      }
    }
    managedResource.flatMap { _.acquireFor(f).convertToTry }
  }

  def write(stream: Iterator[A])(implicit format: IntermediateFormat[A]): Try[Unit] = {
    val path = Paths.get(new URI(url))
    Try(for {
      writer <- managed(Files.newBufferedWriter(path, Charset.defaultCharset(), StandardOpenOption.APPEND))
    } {
      stream.foreach { input =>
        writer.write(toLine(input))
        writer.newLine()
      }
    })
  }
}

case class S3IntermediateConfig(url: String, awsAccessKey: String, awsSecretKey: String, bucketName: String, keyPrefix: String,
    keyDate: DateTime = DateTime.now, columnSeparator: Option[String] = Option("\t")) extends IOConfig

case class S3Intermediate[A](url: String, awsAccessKey: String, awsSecretKey: String, bucketName: String, keyPrefix: String,
    keyDate: DateTime = DateTime.now, override val columnSeparator: Option[String] = Option("\t"))
  extends Intermediate[A]
  with IOOps[A]
  with IntermediateOps
  with Logging {

  val fileChunkSize = 100 * 1024 * 1024 // 100MB
  val dateFormat = DateTimeFormat.forPattern("yyyyMMdd");
  val datedKeyPrefix = s"${keyPrefix}-${keyDate.toString(dateFormat)}"

  def read[B](f: Iterator[A] => B)(implicit format: IntermediateFormat[A]): Try[B] = {
    logger.info(s"Starting stream from S3 with endpoint ${url}")
    val tempFile = Files.createTempFile("waterfall-s3-", ".tsv")
    val keyList = Try(amazonS3Client.listObjects(bucketName, datedKeyPrefix).getObjectSummaries().asScala.map( _.getKey ).toList)
    val bufferedReader = keyList.flatMap { list => Try {
      for {
        key <- list
      } yield {
        amazonS3Client.getObject(new GetObjectRequest(bucketName, key), tempFile.toFile)
      }
    }}.flatMap { _ => Try(Files.newBufferedReader(tempFile, Charset.defaultCharset())) }
    val managedResource = bufferedReader.map { bufReader =>
      for {
        reader <- managed(bufReader)
      } yield {
        Iterator.continually {
          Option(reader.readLine())
        }.takeWhile(_.nonEmpty).map { line =>
          fromLine(line.get)
        }
      }
    }
    managedResource.flatMap { _.acquireFor(f).convertToTry }
  }

  def write(stream: Iterator[A])(implicit format: IntermediateFormat[A]): Try[Unit] = {
    logger.info(s"Starting upload to S3 with endpoint ${url}, starting write with file chunk size ${fileChunkSize}")
    Try(writeChunkToS3(stream, 0)).map { numFiles =>
      logger.info(s"Upload to S3 completed, ${numFiles} files written with file chunk size ${fileChunkSize}")
    }
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
      val uploadFile = Files.createTempFile("waterfall-s3-", "-" + counter + ".tsv")
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