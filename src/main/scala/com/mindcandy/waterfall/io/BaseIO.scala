package com.mindcandy.waterfall.io

import com.mindcandy.waterfall.IntermediateFormat
import com.mindcandy.waterfall.Intermediate
import com.mindcandy.waterfall.FileIntermediate
import com.mindcandy.waterfall.IOConfig
import com.mindcandy.waterfall.IOSource
import com.mindcandy.waterfall.IOSink

case class FileIOConfig(url: String) extends IOConfig

case class FileIO[A](config: FileIOConfig)
  extends IOSource[A]
  with IOSink[A]{

  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    // reusing the FileIntermediate for file reading
    val inputFile = FileIntermediate[A](config.url)
    inputFile.read.acquireFor(intermediate.write(_))
  }
  
  def storeFrom[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    // reusing the FileIntermediate for file writing
    val outputFile = FileIntermediate[A](config.url)
    intermediate.read.acquireFor(outputFile.write(_))
  }
}