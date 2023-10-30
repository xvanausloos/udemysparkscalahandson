version in ThisBuild := "0.1.0-SNAPSHOT"

scalaVersion in ThisBuild := "2.12.14"

lazy val root = (project in file("."))
  .settings(
    name := "udemysparkscalahandson",
    idePackagePrefix := Some("com.ldi.spark")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.3",
  "org.apache.spark" %% "spark-sql" % "3.1.3",
  "org.apache.spark" %% "spark-mllib" % "3.1.3",
  "org.apache.logging.log4j" % "log4j-api" % "2.21.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.21.1"
)
