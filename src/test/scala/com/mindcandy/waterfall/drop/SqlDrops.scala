package com.mindcandy.waterfall.drop

import org.specs2.mutable._
import com.mindcandy.waterfall.io.SqlIOConfig
import com.mindcandy.waterfall.io.FileIOConfig
import com.mindcandy.waterfall.IntermediateFormatCompanion
import com.mindcandy.waterfall.IntermediateFormat
import com.github.nscala_time.time.Imports._
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SqlDropsSpec extends Specification {
  
  "SqlToFile" should {
    "work for a two column test table" in {
      SqlToFileDrop[TestFormat](
        SqlIOConfig("jdbc:postgresql:waterfall", "org.postgresql.Driver", "kevin.schmidt", "", "select * from test_table"),
        FileIOConfig("file:///tmp/test.tsv")
      ).run
      done
    }
  }
}