package com.mindcandy.waterfall.io

import com.mindcandy.waterfall._
import scala.slick.session.Database
import scala.slick.session.Database.threadLocalSession
import scala.slick.jdbc.StaticQuery
import com.typesafe.scalalogging.slf4j.Logging
import scala.util.Try
import com.mindcandy.waterfall.intermediate.S3Intermediate

trait RedshiftIOConfig extends IOConfig {
  def driver: String = "org.postgresql.Driver"
  def username: String
  def password: String
}
case class RedshiftIOSourceConfig(url: String, username: String, password: String, query: String) extends RedshiftIOConfig
case class RedshiftIOSinkConfig(url: String, username: String, password: String, tableName: String, columnNames: Option[List[String]] = None) extends RedshiftIOConfig

case class RedshiftIOSource[A <: AnyRef](config: RedshiftIOSourceConfig, s3Config: Option[S3IOConfig] = None)
    extends IOSource[A]
    with Logging {

  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]): Try[Unit] = {
    val s3Intermediate = intermediate match {
      case i: S3Intermediate[A] => i
      case _ => throw new IllegalArgumentException("currently only S3Intermediate supported")
    }
    val combinedS3Url = s"s3://${s3Intermediate.bucketName}/${s3Intermediate.datedKeyPrefix}/"
    logger.info(s"Copying Redshift query into S3 with url ${combinedS3Url} in db ${config.url}")
    Try(Database.forURL(config.url, driver = config.driver, user = config.username, password = config.password).withSession {
      StaticQuery.updateNA(
        s"UNLOAD ('${config.query.replace("'", "\\\'")}') TO '${combinedS3Url}' CREDENTIALS 'aws_access_key_id=${s3Intermediate.awsAccessKey};aws_secret_access_key=${s3Intermediate.awsSecretKey}' NULL AS '\\\\N' DELIMITER '\\t'"
      ).execute
    })
  }
}

case class RedshiftIOSink[A <: AnyRef](config: RedshiftIOSinkConfig, s3Config: Option[S3IOConfig] = None)
    extends IOSink[A]
    with Logging {

  def storeFrom[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    val s3Intermediate = intermediate match {
      case i: S3Intermediate[A] => i
      case _ => throw new IllegalArgumentException("currently only S3Intermediate supported")
    }
    val combinedS3Url = s"s3://${s3Intermediate.bucketName}/${s3Intermediate.datedKeyPrefix}"
    val columns = config.columnNames.map("(" + _.mkString(",") + ")").getOrElse("")
    logger.info(s"Copying S3 data from ${combinedS3Url} into Redshift table ${config.tableName} in db ${config.url}")
    Try(Database.forURL(config.url, driver = config.driver, user = config.username, password = config.password).withSession {
      StaticQuery.updateNA(
        s"COPY ${config.tableName} ${columns} FROM '${combinedS3Url}' CREDENTIALS 'aws_access_key_id=${s3Intermediate.awsAccessKey};aws_secret_access_key=${s3Intermediate.awsSecretKey}' DELIMITER '\\t'"
      ).execute
    })
  }
}