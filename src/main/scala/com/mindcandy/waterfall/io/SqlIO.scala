package com.mindcandy.waterfall.io

import scala.slick.session.Database
import scala.slick.session.Database.threadLocalSession
import scala.slick.jdbc.{ GetResult, StaticQuery }
import scala.slick.driver.PostgresDriver.simple._
import scala.slick.session.PositionedResult
import com.mindcandy.waterfall.IntermediateFormat
import com.mindcandy.waterfall.Intermediate
import com.mindcandy.waterfall.IOConfig
import com.mindcandy.waterfall.IOSource
import com.typesafe.scalalogging.slf4j.Logging
import resource._

case class SqlIOConfig(val url: String, val driver: String, val username: String, val password: String, val query: String) extends IOConfig {
  override def toString = "SqlIOConfig(%s, %s, %s)".format(url, driver, query)
}

case class SqlIOSource[A](config: SqlIOConfig) extends IOSource[A] with Logging {
  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    logger.info("Sourcing from %s".format(config))
    Database.forURL(config.url, driver = config.driver) withSession {
      val getResult = {
        GetResult(r => {
          loop(Seq[String](), r)
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
  private def loop(result: Seq[String], resultSet: PositionedResult): Seq[String] = {
    if (resultSet.hasMoreColumns)
      loop(result :+ resultSet.nextString, resultSet)
    else
      result
  }
}