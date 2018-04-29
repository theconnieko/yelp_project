name := "yelp_project"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
//  "com.typesafe.play" %% "play-json" % "2.6.7",
  "com.github.nscala-time" %% "nscala-time" % "2.18.0",
  "joda-time" % "joda-time" % "2.9.9",
  "org.apache.spark" % "spark-core_2.11" % "2.2.0" % "provided",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0" % "provided"
)

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"

//assemblyExcludedJars in assembly := {
//  val cp = (fullClasspath in assembly).value
//  cp filter { f =>
//    f.data.getName.contains("spark-core") ||
//      f.data.getName == "spark-core_2.11-2.0.1.jar"
//  }
//}