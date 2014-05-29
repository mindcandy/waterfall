package com.mindcandy.waterfall.drop

import com.mindcandy.waterfall.io.SqlIOSource
import com.mindcandy.waterfall.io.SqlIOConfig
import com.mindcandy.waterfall.io.FileIO
import com.mindcandy.waterfall.IOConfig

trait SqlToFileDrop[A] extends PassThroughWaterfallDrop[A] {  
  def sourceConfig : SqlIOConfig
  def source = SqlIOSource[A](sourceConfig)
  def sinkConfig : IOConfig
  def sink = FileIO[A](sinkConfig)
}