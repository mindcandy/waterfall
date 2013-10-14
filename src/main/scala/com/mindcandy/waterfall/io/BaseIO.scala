package com.mindcandy.waterfall.io

case class FileIOConfig(url: String) extends IOConfig

case class FileIO[A](config: FileIOConfig)
  extends IOSource[A, FileIOConfig]
  with IOSink[A, FileIOConfig]{

  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit toConverter: Seq[String] => A, fromConverter: A => Seq[String]) = {
    // not implemented
  }
  
  def storeInto[I <: Intermediate[A]](intermediate: I)(implicit toConverter: (Seq[String] => A), fromConverter: (A => Seq[String])) = {
    // not implemented
  }
}