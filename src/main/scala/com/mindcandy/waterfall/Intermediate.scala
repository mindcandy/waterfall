package com.mindcandy.waterfall
import scala.reflect._
import resource._
import java.nio.file.Files
import java.nio.charset.Charset
import java.nio.file.Paths
import java.net.URI
import java.io._

trait IntermediateFormatCompanion[A] {
  implicit def format: IntermediateFormat[A]
}

trait IntermediateFormat[A] {
  def convertTo(value: Seq[String]): A
  def convertFrom(value: A): Seq[String]
}

sealed trait Intermediate[A] {
  def url: String
  def read(implicit format: IntermediateFormat[A]): ManagedResource[Iterator[A]]
  def write(stream: Iterator[A])(implicit format: IntermediateFormat[A]): Unit
}

case class MemoryIntermediate[A](url: String) extends Intermediate[A] {
  var data = Seq[Seq[String]]()

  implicit def SeqResource[B <: Seq[_]] = new Resource[B] {
    override def close(r: B) = ()
  }

  def read(implicit format: IntermediateFormat[A]): ManagedResource[Iterator[A]] = {
    for {
      reader <- managed(data)
    } yield {
      reader.map(format.convertTo).iterator
    }
  }
  def write(value: Iterator[A])(implicit format: IntermediateFormat[A]): Unit = {
    data = value.map { format.convertFrom(_) }.toSeq
  }
}

case class FileIntermediate[A](url: String) extends Intermediate[A] {
  def columnSeparator = "\t"
  def rowSeparator = "\n"

  def read(implicit format: IntermediateFormat[A]): ManagedResource[Iterator[A]] = {
    val path = Paths.get(new URI(url))
    for {
      reader <- managed(Files.newBufferedReader(path, Charset.defaultCharset()))
    } yield {
      Iterator.continually(Option(reader.readLine())).takeWhile(_.nonEmpty).map { line => format.convertTo((line.get.split(columnSeparator))) }
    }
  }

  def write(value: Iterator[A])(implicit format: IntermediateFormat[A]): Unit = {
    val path = Paths.get(new URI(url))
    for {
      writer <- managed(Files.newBufferedWriter(path, Charset.defaultCharset()))
    } {
      value.foreach { input =>
        val rawInput = format.convertFrom(input)
        val finalInput = rawInput.tail.foldLeft(rawInput.head)("%s%s%s".format(_, columnSeparator, _))
        writer.write(finalInput + rowSeparator)
      }
    }
  }
}