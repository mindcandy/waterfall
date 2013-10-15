package com.mindcandy.waterfall.io

import scala.io.Source
import scala.reflect._
import scala.language.implicitConversions
import resource._
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.FileSystems
import java.nio.charset.Charset
import java.io.FileInputStream
import java.nio.file.Paths
import java.net.URI
import java.io._

trait IntermediateFormat[A] {
  implicit def toConverter(value: Seq[String]): A
  implicit def fromConverter(value: A): Seq[String]
}

sealed trait Intermediate[A] {
  def url: String
  def read(implicit converter: Seq[String] => A): ManagedResource[Iterator[A]]
  def write(stream: Iterator[A])(implicit converter: A => Seq[String]): Unit
}

case class MemoryIntermediate[A](url: String) extends Intermediate[A] {
  var data = Seq[Seq[String]]()

  implicit def SeqResource[B <: Seq[_]] = new Resource[B] {
    override def close(r: B) = ()
  }

  def read(implicit converter: Seq[String] => A): ManagedResource[Iterator[A]] = {
    for {
      reader <- managed(data)
    } yield {
      reader.map(converter).iterator
    }
  }
  def write(value: Iterator[A])(implicit converter: A => Seq[String]): Unit = data = value.map { converter(_) }.toSeq
}

case class FileIntermediate[A](url: String) extends Intermediate[A] {

  def read(implicit converter: Seq[String] => A): ManagedResource[Iterator[A]] = {
    val path = Paths.get(new URI(url))
    for {
      reader <- managed(Files.newBufferedReader(path, Charset.defaultCharset()))
    } yield {
      Iterator.continually(Option(reader.readLine())).takeWhile(_.nonEmpty).map { line => converter(line.get.split("\t")) }
    }
  }

  def write(value: Iterator[A])(implicit converter: A => Seq[String]): Unit = {
    val path = Paths.get(new URI(url))
    for {
      writer <- managed(Files.newBufferedWriter(path, Charset.defaultCharset()))
    } {
      value.foreach { input =>
        val rawInput = converter(input)
        val finalInput = rawInput.tail.foldLeft(rawInput.head)("%s\t%s".format(_, _))
        writer.write(finalInput + "\n")
      }
    }
  }
}