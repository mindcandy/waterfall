package com.mindcandy.waterfall.drop

import com.mindcandy.waterfall.io.SqlIOSource
import com.mindcandy.waterfall.io.SqlIOConfig
import com.mindcandy.waterfall.FileIntermediate
import com.mindcandy.waterfall.io.FileIO
import com.mindcandy.waterfall.io.FileIOConfig
import com.mindcandy.waterfall.IOSink
import com.mindcandy.waterfall.IOConfig
import java.io.File
import com.mindcandy.waterfall.PassThroughWaterfallDrop
import com.mindcandy.waterfall.io.S3IO
import com.mindcandy.waterfall.io.S3IOConfig

case class SqlToFileDrop[A](sourceConfig: SqlIOConfig, sinkConfig: FileIOConfig) extends PassThroughWaterfallDrop[A] {  
  def source = SqlIOSource[A](sourceConfig)
  def sink = FileIO[A](sinkConfig)
}

case class SqlToS3Drop[A](sourceConfig: SqlIOConfig, sinkConfig: S3IOConfig) extends PassThroughWaterfallDrop[A] {  
  def source = SqlIOSource[A](sourceConfig)
  def sink = S3IO[A](sinkConfig)
}