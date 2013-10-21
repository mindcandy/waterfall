package com.mindcandy.waterfall.io

import com.mindcandy.waterfall.IntermediateFormat
import com.mindcandy.waterfall.Intermediate
import com.mindcandy.waterfall.FileIntermediate
import com.mindcandy.waterfall.S3Intermediate
import com.mindcandy.waterfall.IOConfig
import com.mindcandy.waterfall.IOSource
import com.mindcandy.waterfall.IOSink

case class FileIOConfig(url: String) extends IOConfig
case class S3IOConfig(url: String, awsAccessKey: String, awsSecretKey: String, bucketName: String, keyPrefix: String) extends IOConfig

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

case class S3IO[A](config: S3IOConfig)
  extends IOSource[A]
  with IOSink[A]{

  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    // reusing the S3Intermediate for file reading
    val inputFile = S3Intermediate[A](config.url, config.awsAccessKey, config.awsSecretKey, config.bucketName, config.keyPrefix)
    inputFile.read.acquireFor(intermediate.write(_))
  }
  
  def storeFrom[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    // reusing the S3Intermediate for file writing
    val outputFile = S3Intermediate[A](config.url, config.awsAccessKey, config.awsSecretKey, config.bucketName, config.keyPrefix)
    intermediate.read.acquireFor(outputFile.write(_))
  }
}