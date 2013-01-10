name := "baseline"

version := "0.1"

scalaVersion := "2.10.0"

// tests are not thread safe
parallelExecution in Test := false

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0.M5b" % "test"

