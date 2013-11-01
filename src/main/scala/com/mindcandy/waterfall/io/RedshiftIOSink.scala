package com.mindcandy.waterfall.io

import com.mindcandy.waterfall.IOSource
import com.mindcandy.waterfall.IOSink
import com.mindcandy.waterfall.Intermediate
import com.mindcandy.waterfall.IntermediateFormat
import com.mindcandy.waterfall.IOConfig
import com.mindcandy.waterfall.S3Intermediate
import scala.slick.session.Database
import scala.slick.session.Database.threadLocalSession
import scala.slick.jdbc.StaticQuery
import com.typesafe.scalalogging.slf4j.Logging

case class RedshiftIOConfig(url: String, driver: String, username: String, password: String, tableName: String) extends IOConfig

case class RedshiftIOSink[A](config: RedshiftIOConfig, s3Config: Option[S3IOConfig] = None)
  extends IOSink[A]
  with Logging {

  def storeFrom[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    val s3Intermediate = intermediate match {
      case i: S3Intermediate[A] => i
      case _ => throw new IllegalArgumentException("currently only S3Intermediate supported")
    }
    val combinedS3Url = s"s3://${s3Intermediate.bucketName}/${s3Intermediate.datedKeyPrefix}"
    logger.info(s"Copying S3 data from ${combinedS3Url} into Redshift table ${config.tableName} in db ${config.url}")
    Database.forURL(config.url, driver = config.driver, user = config.username, password = config.password) withSession {
      StaticQuery.updateNA(
        s"COPY ${config.tableName} FROM '${combinedS3Url}' credentials 'aws_access_key_id=${s3Intermediate.awsAccessKey};aws_secret_access_key=${s3Intermediate.awsSecretKey}' DELIMITER '\\t'"
      ).execute
    }
  }
}