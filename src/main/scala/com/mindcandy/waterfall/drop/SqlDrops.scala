package com.mindcandy.waterfall.drop

import com.mindcandy.waterfall.io.SqlIOSource
import com.mindcandy.waterfall.io.SqlIOConfig
import com.mindcandy.waterfall.FileIntermediate
import com.mindcandy.waterfall.io.FileIO
import com.mindcandy.waterfall.IOSink
import com.mindcandy.waterfall.IOConfig
import java.io.File
import com.mindcandy.waterfall.PassThroughWaterfallDrop
import com.mindcandy.waterfall.io.S3IO
import com.mindcandy.waterfall.io.S3IOConfig

trait SqlToFileDrop[A] extends PassThroughWaterfallDrop[A] {  
  def sourceConfig : SqlIOConfig
  def source = SqlIOSource[A](sourceConfig)
  def sinkConfig : IOConfig
  def sink = FileIO[A](sinkConfig)
}