import sbt._
import sbt.Keys._

object WaterfallBuild extends Build {

  lazy val waterfall = Project(
    id = "waterfall",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "waterfall",
      organization := "com.mindcandy",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.10.2"
      // add other settings here
    )
  )
}
