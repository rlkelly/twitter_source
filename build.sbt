import scala.sys.process._

name := "brainsharesource"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.4.0" % "provided",
  "org.apache.spark" % "spark-sql_2.11" % "2.4.0" % "provided"
)

libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.4.2"

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", xs@_*) => MergeStrategy.last
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyJarName := s"${name.value}-${version.value}.jar"

lazy val runDemo = taskKey[Unit]("Execute the shell script")

runDemo := {
  "spark-submit --class com.brainshare.App ./target/scala-2.11/brainsharesource_2.11-0.1.jar" !
}
