package com.mindcandy.waterfall.transform

import com.mindcandy.waterfall.Transformer
import com.mindcandy.waterfall.io.SqlIOSource
import com.mindcandy.waterfall.io.SqlIOConfig
import com.mindcandy.waterfall.FileIntermediate
import com.mindcandy.waterfall.io.FileIO
import com.mindcandy.waterfall.io.FileIOConfig
import com.mindcandy.waterfall.IOSink
import com.mindcandy.waterfall.IOConfig
import java.io.File
import com.mindcandy.waterfall.PassThroughTransformer

case class SqlToFile[A](sourceConfig: SqlIOConfig, sinkConfig: FileIOConfig) extends PassThroughTransformer[A] {
  override val sharedIntermediate = FileIntermediate[A]("file:///tmp/diablo-test.tsv")
  
  def source = SqlIOSource[A](sourceConfig)
  def sink = FileIO[A](sinkConfig)
}