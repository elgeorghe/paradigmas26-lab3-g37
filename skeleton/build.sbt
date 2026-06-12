name := "reddit-ner-scala"

version := "0.1.0"

scalaVersion := "2.13.18"

ThisBuild / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat

fork := true

ThisBuild / javacOptions ++= Seq("-source", "17", "-target", "17")
ThisBuild / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-exports=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.math/java.math=ALL-UNNAMED" //?
)

libraryDependencies ++= Seq(
  "org.json4s" %% "json4s-jackson" % "3.7.0-M11",
  "com.github.scopt" %% "scopt" % "4.1.0",
  "org.apache.spark" %% "spark-sql" % "3.4.1"
)


javaHome := Some(file("/usr/lib/jvm/java-17-openjdk-amd64"))

Global / excludeLintKeys ++= Set(
  ThisBuild / classLoaderLayeringStrategy
)