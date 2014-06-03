package com.mindcandy.waterfall.intermediate

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.GetObjectRequest
import com.github.nscala_time.time.Imports._
import com.mindcandy.waterfall._
import com.typesafe.scalalogging.slf4j.Logging
import java.nio.charset.Charset
import java.nio.file.{ FileSystems, Files }
import resource._
import scala.annotation.tailrec
import scala.util.Try
import scala.collection.JavaConverters._

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
    val keyList = Try(amazonS3Client.listObjects(bucketName, datedKeyPrefix).getObjectSummaries().asScala.map(_.getKey).toList)
    val bufferedReader = keyList.flatMap { list =>
      Try {
        for {
          key <- list
        } yield {
          amazonS3Client.getObject(new GetObjectRequest(bucketName, key), tempFile.toFile)
        }
      }
    }.flatMap { _ => Try(Files.newBufferedReader(tempFile, Charset.defaultCharset())) }
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
        while (stream.hasNext && byteCounter < fileChunkSize) {
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