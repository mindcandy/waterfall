import sbt._
import sbt.Keys._

object WaterfallBuild extends Build {

  lazy val basicDependencies: Seq[Setting[_]] = Seq(
    libraryDependencies ++= Seq("com.typesafe.slick" %% "slick" % "1.0.1"),
    libraryDependencies += "postgresql" % "postgresql" % "9.1-901.jdbc4",
    libraryDependencies += "com.jolbox" % "bonecp" % "0.7.1.RELEASE",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.0.13",
    libraryDependencies += "com.typesafe" %% "scalalogging-slf4j" % "1.0.1",
    libraryDependencies += "com.jsuereth" %% "scala-arm" % "1.3",
    libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "0.6.0")

  lazy val testDependencies: Seq[Setting[_]] = Seq(
    libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.10.1" % "test",
    libraryDependencies += "org.mockito" % "mockito-all" % "1.9.5" % "test",
    libraryDependencies += "org.specs2" %% "specs2" % "2.2.3" % "test")

  lazy val waterfall = Project(
    id = "waterfall",
    base = file("."),
    settings = Project.defaultSettings ++ basicDependencies ++ testDependencies ++ Seq(
      name := "waterfall",
      organization := "com.mindcandy",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.10.2" // add other settings here
      ))
}
