package com.mindcandy.waterfall.io

import java.io.{ SequenceInputStream, InputStream, InputStreamReader, BufferedReader }
import resource._
import scala.collection.JavaConverters._
import scala.util.{ Try, Failure }
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.mindcandy.waterfall.RowSeparator.RowSeparator
import com.mindcandy.waterfall._
import com.mindcandy.waterfall.RowSeparator._

case class S3IOConfig(url: String, awsAccessKey: String, awsSecretKey: String, bucketName: String, keyPrefix: String) extends IOConfig

case class S3IO[A <: AnyRef](config: S3IOConfig, val keySuffix: Option[String] = None, val columnSeparator: Option[String] = Option("\t"), val rowSeparator: RowSeparator = NewLine)
    extends IOSource[A]
    with IOOps[A]
    with IntermediateOps {

  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    val keyPrefix = config.keyPrefix + keySuffix.getOrElse("")
    val computedKeys = amazonS3Client.map {
      _.listObjects(config.bucketName, keyPrefix).getObjectSummaries.asScala.toList.map(_.getKey)
    }
    val bufferedReader = amazonS3Client.flatMap { s3Client =>
      computedKeys.flatMap {
        case Nil => Failure(new Exception(s"No keys found in ${config.bucketName} for key prefix ${keyPrefix}"))
        case keys => Try {
          logger.info(s"Starting stream from S3 with endpoint ${config.url} using ${config.bucketName} and keys ${keys}")
          val inputStreams = keys.map(s3Client.getObject(config.bucketName, _).getObjectContent())
          new BufferedReader(new InputStreamReader(inputStreams.reduceLeft[InputStream] { (input1, input2) =>
            new SequenceInputStream(input1, input2)
          }))
        }
      }
    }
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

  val amazonS3Client = Try {
    val awsCredentials = new BasicAWSCredentials(config.awsAccessKey, config.awsSecretKey)
    val s3Client = new AmazonS3Client(awsCredentials)
    s3Client.setEndpoint(config.url)
    s3Client
  }
}