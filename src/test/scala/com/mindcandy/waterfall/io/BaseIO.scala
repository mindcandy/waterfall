package com.mindcandy.waterfall.io

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.mock.Mockito
import org.specs2.runner.JUnitRunner
import com.mindcandy.waterfall.TestFormat
import com.mindcandy.waterfall.Intermediate
import resource.ManagedResource
import com.mindcandy.waterfall.IntermediateFormat
import resource._
import java.io.IOException

@RunWith(classOf[JUnitRunner])
class BaseIOSpec extends Specification with Mockito {
  
  case class FailingIntermediate[A](url: String) extends Intermediate[A] {
    var data = Seq[Seq[String]]()

    implicit def SeqResource[B <: Seq[_]] = new Resource[B] {
      override def close(r: B) = ()
    }

    def read(implicit format: IntermediateFormat[A]): ManagedResource[Iterator[A]] = {
      for { reader <- managed(data) } yield {
        throw new Exception("this intermediate will always fail")
      }
    }

    def write(value: Iterator[A])(implicit format: IntermediateFormat[A]): Unit = {
      throw new Exception("this intermediate will always fail")
    }
  }

  "FileIO" should {
    "fail storeForm with an io exception if the underlying intermediate cannot read" in {
      val fileIO = FileIO[TestFormat](FileIOConfig("/tmp/waterfall-test-file.tsv"))
      fileIO.storeFrom(FailingIntermediate[TestFormat]("nothing")) must throwA[IOException]
    }
  }
}