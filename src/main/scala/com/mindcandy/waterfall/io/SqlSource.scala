package com.mindcandy.waterfall.io

import com.jolbox.bonecp.BoneCPConfig
import com.jolbox.bonecp.BoneCPDataSource
import scala.slick.session.Database
import scala.slick.session.Database.threadLocalSession
import scala.slick.jdbc.{ GetResult, StaticQuery }
import scala.slick.driver.PostgresDriver.simple._
import scala.slick.session.PositionedResult

case class SqlIOConfig(val url: String, val driver: String, val username: String, val password: String, val query: String) extends IOConfig

case class SqlSource[A](config: SqlIOConfig) extends IOSource[A, SqlIOConfig] {
  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit toConverter: (Seq[String] => A), fromConverter: (A => Seq[String])) = {
    Database.forURL(config.url, driver = config.driver) withSession {
      //def query(): Stream[A] = sql"select * from test_table".as[A].toStream
      //query().foreach(result => doSomething());
      // val s = Stream[List[String]]
      // s.foreach(writer.write)
      implicit val getResult = {
        GetResult(r => {
          loop(Seq[String](), r)
        })
      }
      val q = StaticQuery.queryNA(config.query).to[Seq]
      val result = q.map { toConverter(_) }
      intermediate.write(result.iterator)(fromConverter)
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