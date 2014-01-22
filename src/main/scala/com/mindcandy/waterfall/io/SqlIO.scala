package com.mindcandy.waterfall.io

import scala.slick.session.Database
import scala.slick.session.Database.threadLocalSession
import scala.slick.jdbc.{ GetResult, StaticQuery }
import scala.slick.session.PositionedResult
import com.mindcandy.waterfall.IntermediateFormat
import com.mindcandy.waterfall.Intermediate
import com.mindcandy.waterfall.IOConfig
import com.mindcandy.waterfall.IOSource
import com.typesafe.scalalogging.slf4j.Logging
import resource._
import com.mindcandy.waterfall.MemoryIntermediate

case class SqlIOConfig(url: String, driver: String, username: String, password: String, query: String) extends IOConfig {
  override def toString = "SqlIOConfig(%s, %s, %s)".format(url, driver, query)
}

case class SqlIOSource[A](config: SqlIOConfig) extends IOSource[A] with Logging {
  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    logger.info("Sourcing from %s".format(config))
    Database.forURL(config.url, driver = config.driver, user = config.username, password = config.password) withSession {
      val getResult = {
        GetResult(r => {
          processResultSet(Seq[String](), r)
        })
      }
      val result = managed(StaticQuery.queryNA(config.query)(getResult).elements)
      result.acquireFor { iterator =>
        intermediate.write {
          iterator.map(format.convertTo(_))
        }
      } match {
        case Left(exceptions) => handleErrors(exceptions)
        case Right(result) => logger.info("Retrieving into %s from %s completed".format(intermediate, config))
      }
    }
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
  def driver: String
  def username: String
  def password: String
  def queries(url: String): List[String]
  override def url = urls.mkString(";")
  override def toString = "ShardedSqlIOConfig(%s, %s)".format(urls, driver)
}

case class ShardedSqlIOSource[A](config: ShardedSqlIOConfig) extends IOSource[A] with Logging {
  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    val combinedIntermediate = MemoryIntermediate[A]("memory:shardedsqliosource:temp")
    generateSqlIOConfigs(config).foreach( SqlIOSource[A](_).retrieveInto(combinedIntermediate)(format) )
    combinedIntermediate.read(format).acquireFor( intermediate.write(_) ) match {
      case Left(exceptions) => handleErrors(exceptions)
      case Right(result) => logger.info("Retrieving into %s from %s completed".format(intermediate, config))
    }
  }
 
  def generateSqlIOConfigs(config: ShardedSqlIOConfig) = {
    config.urls.flatMap { url => config.queries(url) map { query =>
        SqlIOConfig(url, config.driver, config.username, config.password, query)
      }
    }
  }
}