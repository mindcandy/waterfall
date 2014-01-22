package com.mindcandy.waterfall.io

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import com.mindcandy.waterfall.SimpleTestFormat
import com.mindcandy.waterfall.MemoryIntermediate

@RunWith(classOf[JUnitRunner])
class SqlIOSpec extends Specification {
  
  "SqlIOSource" should {
    "work for a two column test table" in {
      val intermediate = MemoryIntermediate[SimpleTestFormat]("memory:testintermediate")
      SqlIOSource[SimpleTestFormat](
        SqlIOConfig("jdbc:postgresql:waterfall", "org.postgresql.Driver", "kevin.schmidt", "", "select * from test_table")
      ).retrieveInto(intermediate)
      intermediate.data.toList must_== List(
        List("1", "Middleware Team"),
        List("2", "Tools Team"),
        List("3", "Moshi Team")
      )
    }
  }
  
  "ShardedSqlIOSource" should {
    "work for a two column test table across two databases with two tables" in {
      case class TestShardedSqlIOConfig() extends ShardedSqlIOConfig {
        val urls = List("jdbc:postgresql:waterfall", "jdbc:postgresql:waterfall_sharded")
        val driver = "org.postgresql.Driver"
        val username = "kevin.schmidt"
        val password = ""
        def queries(url: String) = {
          url match {
            case "jdbc:postgresql:waterfall" => List("select * from test_table", "select * from test_table_sharded2")
            case "jdbc:postgresql:waterfall_sharded" => List("select * from test_table_sharded3", "select * from test_table_sharded4")
          }
        }
      }
      
      val intermediate = MemoryIntermediate[SimpleTestFormat]("memory:testintermediate")
      ShardedSqlIOSource[SimpleTestFormat](TestShardedSqlIOConfig()).retrieveInto(intermediate)
      intermediate.data.toList must_== List(
        List("1", "Middleware Team"),
        List("2", "Tools Team"),
        List("3", "Moshi Team"),
        List("4", "Village Team"),
        List("5", "Rescue Team"),
        List("6", "Warriors Team")
      )
    }
  }
}