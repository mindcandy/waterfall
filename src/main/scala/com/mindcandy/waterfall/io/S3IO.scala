package com.mindcandy.waterfall.io

import java.io.{ BufferedReader, InputStreamReader }

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.mindcandy.waterfall.RowSeparator._
import com.mindcandy.waterfall._
import com.mindcandy.waterfall.intermediate.S3Ops
import resource._

import scala.util.Try

case class S3IOConfig(url: String, awsAccessKey: String, awsSecretKey: String, bucketName: String, keyPrefix: String) extends IOConfig {
  override def toString = "S3IOConfig(%s, %s, %s)".format(url, bucketName, keyPrefix)
}

case class S3IO[A <: AnyRef](config: S3IOConfig, val keySuffix: Option[String] = None, val columnSeparator: Option[String] = Option("\t"), val rowSeparator: RowSeparator = NewLine)
    extends IOSource[A]
    with IOOps[A]
    with IntermediateOps
    with S3Ops {

  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    val keyPrefix = config.keyPrefix + keySuffix.getOrElse("")
    val computedKeys = amazonS3Client.map { s3Client => getObjects(Nil, s3Client.listObjects(config.bucketName, keyPrefix), s3Client) }
    computedKeys.flatMap { keys =>
      logger.info(s"Starting stream from S3 with endpoint ${config.url} using ${config.bucketName} and keys ${keys}")
      keys.foldLeft(Try(())) { (result, key) =>
        result.flatMap { _ =>
          amazonS3Client.flatMap { s3Client =>
            val inputContent = for {
              reader <- managed(new BufferedReader(new InputStreamReader(s3Client.getObject(config.bucketName, key).getObjectContent())))
            } yield {
              val rawData = Iterator.continually {
                Option(reader.readLine())
              }.takeWhile(_.nonEmpty).flatten
              processRowSeparator(rawData, rowSeparator)
            }
            inputContent.acquireFor(intermediate.write).convertToTry.map { _ =>
              logger.info("Retrieving into %s from %s for key %s completed".format(intermediate, config, key))
            }
          }
        }
      }
    }
  }

  val amazonS3Client = Try {
    val awsCredentials = new BasicAWSCredentials(config.awsAccessKey, config.awsSecretKey)
    val s3Client = new AmazonS3Client(awsCredentials)
    s3Client.setEndpoint(config.url)
    s3Client
  }
}