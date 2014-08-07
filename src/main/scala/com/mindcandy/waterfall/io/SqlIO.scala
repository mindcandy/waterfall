package com.mindcandy.waterfall.io

import scala.slick.jdbc.JdbcBackend.Database
import scala.slick.jdbc.JdbcBackend.Database.dynamicSession
import scala.slick.jdbc.{ GetResult, StaticQuery, PositionedResult }
import com.mindcandy.waterfall.IntermediateFormat
import com.mindcandy.waterfall.Intermediate
import com.mindcandy.waterfall.IOConfig
import com.mindcandy.waterfall.IOSource
import com.typesafe.scalalogging.slf4j.Logging
import com.mindcandy.waterfall.intermediate.FileIntermediate
import scala.util.Try

case class SqlIOConfig(url: String, driver: String, username: String, password: String, query: String) extends IOConfig {
  override def toString = "SqlIOConfig(%s, %s, %s)".format(url, driver, query)
}

case class SqlIOSource[A <: AnyRef](config: SqlIOConfig) extends IOSource[A] with Logging {
  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    logger.info("Sourcing from %s".format(config))
    Try(Database.forURL(config.url, driver = config.driver, user = config.username, password = config.password) withDynSession {
      val getResult = {
        GetResult(r => {
          processResultSet(Seq[String](), r)
        })
      }
      val result = StaticQuery.queryNA(config.query)(getResult).iterator
      intermediate.write { result.map(format.convertTo(_)) }
      logger.info("Retrieving into %s from %s completed".format(intermediate, config))
    })
  }

  @annotation.tailrec
  private def processResultSet(result: Seq[String], resultSet: PositionedResult): Seq[String] = {
    if (resultSet.hasMoreColumns)
      processResultSet(result :+ resultSet.nextString, resultSet)
    else
      result
  }
}

trait ShardedSqlIOConfig extends IOConfig {
  def urls: List[String]
  def combinedFileUrl: String
  def driver: String
  def username: String
  def password: String
  def queries(url: String): List[String]
  override def url = urls.mkString(";")
  override def toString = "ShardedSqlIOConfig(%s, %s)".format(urls, driver)
}

case class ShardedSqlIOSource[A <: AnyRef](config: ShardedSqlIOConfig) extends IOSource[A] with Logging {
  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    val combinedIntermediate = FileIntermediate[A](config.combinedFileUrl)
    val result = generateSqlIOConfigs(config).foldLeft(Try(())) { (previousResult, sqlConfig) =>
      previousResult.flatMap { _ =>
        SqlIOSource[A](sqlConfig).retrieveInto(combinedIntermediate)(format)
      }
    }
    result.flatMap { _ =>
      combinedIntermediate.read(intermediate.write)(format).map { _ =>
        logger.info("Retrieving into %s from %s completed".format(intermediate, config))
      }
    }
  }

  def generateSqlIOConfigs(config: ShardedSqlIOConfig) = {
    config.urls.flatMap { url =>
      config.queries(url) map { query =>
        SqlIOConfig(url, config.driver, config.username, config.password, query)
      }
    }
  }
}