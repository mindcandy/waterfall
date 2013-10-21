package com.mindcandy.waterfall

import resource._
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

trait IntermediateFormatCompanion[A] {
  implicit def format: IntermediateFormat[A]
}

trait IntermediateFormat[A] {
  def convertTo(value: Seq[String]): A
  def convertFrom(value: A): Seq[String]
}

trait Intermediate[A] {
  def url: String
  def read(implicit format: IntermediateFormat[A]): ManagedResource[Iterator[A]]
  def write(stream: Iterator[A])(implicit format: IntermediateFormat[A]): Unit
  
  def columnSeparator = "\t"
  def rowSeparator = "\n"
  def toLine(input: A)(implicit format: IntermediateFormat[A]) = {
    val rawInput = format.convertFrom(input)
    val finalInput = rawInput.tail.foldLeft(rawInput.head)("%s%s%s".format(_, columnSeparator, _))
    finalInput + rowSeparator
  }
}

case class MemoryIntermediate[A](url: String) extends Intermediate[A] {
  var data = Seq[Seq[String]]()

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
  def write(value: Iterator[A])(implicit format: IntermediateFormat[A]): Unit = {
    data = value.map { format.convertFrom(_) }.toSeq
  }
}

case class FileIntermediate[A](url: String) extends Intermediate[A] with Logging {
  def read(implicit format: IntermediateFormat[A]): ManagedResource[Iterator[A]] = {
    val path = Paths.get(new URI(url))
    logger.info("Starting read from file intermediate with path [%s]".format(path))
    for {
      reader <- managed(Files.newBufferedReader(path, Charset.defaultCharset()))
    } yield {
      Iterator.continually(Option(reader.readLine())).takeWhile(_.nonEmpty).map { line => format.convertTo((line.get.split(columnSeparator))) }
    }
  }

  def write(value: Iterator[A])(implicit format: IntermediateFormat[A]): Unit = {
    val path = Paths.get(new URI(url))
    logger.info("Starting write to file intermediate with path [%s]".format(path))
    for {
      writer <- managed(Files.newBufferedWriter(path, Charset.defaultCharset()))
    } {
      value.foreach { input =>
        writer.write(toLine(input))
      }
    }
  }
}

case class S3Intermediate[A](url: String, awsAccessKey: String, awsSecretKey: String, bucketName: String, keyPrefix: String) extends Intermediate[A] with Logging {
  def fileChunkSize = 100*1024*1024 // 100MB
    
  def read(implicit format: IntermediateFormat[A]): ManagedResource[Iterator[A]] = {
    val path = Paths.get(new URI(url))
    for {
      reader <- managed(Files.newBufferedReader(path, Charset.defaultCharset()))
    } yield {
      Iterator.continually(Option(reader.readLine())).takeWhile(_.nonEmpty).map { line => format.convertTo((line.get.split(columnSeparator))) }
    }
  }

  def write(value: Iterator[A])(implicit format: IntermediateFormat[A]): Unit = {
    logger.info("Starting upload to S3 intermediate with endpoint %s, starting write with file chunk size %d".format(url, fileChunkSize))
    val numFiles = writeChunkToS3(value, 1)
    logger.info("Upload to S3 intermediate completed, %d files written with file chunk size %d".format(numFiles, fileChunkSize))
  }
  
  lazy val amazonS3Client = {
    val awsCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey)
    val s3Client = new AmazonS3Client(awsCredentials)
    s3Client.setEndpoint(url)
    s3Client
  }
  
  @tailrec
  private[this] def writeChunkToS3(value: Iterator[A], counter: Int)(implicit format: IntermediateFormat[A]) : Int = {
    val uploadFile = Files.createTempFile("waterfall-", "-" + counter + ".tsv")
    var byteCounter = 0
    for {
      writer <- managed(Files.newBufferedWriter(uploadFile, Charset.defaultCharset()))
    } {
      value.takeWhile(_ => byteCounter < fileChunkSize).foreach { input =>
        val line = toLine(input)
        writer.write(line)
        byteCounter += line.getBytes("UTF-8").length
      }
    }
    if (byteCounter == 0) return counter-1;
    logger.info("Finished writing %d bytes to temporary file %s".format(byteCounter, uploadFile))
    val keyName = "%s-%d.tsv".format(keyPrefix, counter)
    logger.info("Starting S3 upload to bucket/key: %s/%s".format(bucketName, keyName))
    amazonS3Client.putObject(bucketName, keyName, uploadFile.toFile)
    return writeChunkToS3(value, counter+1)
  }
}