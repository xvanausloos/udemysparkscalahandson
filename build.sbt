version in ThisBuild := "0.1.0-SNAPSHOT"

scalaVersion in ThisBuild := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "udemy-sparkscalahandson",
    idePackagePrefix := Some("com.ldi.spark")
  )
