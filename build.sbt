name := "WB"

version := "0.1"

scalaVersion := "2.12.14"

idePackagePrefix := Some("wb.test")

val spark = "3.1.2"

libraryDependencies += "org.apache.spark" %% "spark-sql" % spark
libraryDependencies += "org.apache.spark" %% "spark-core" % spark


libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % s"${spark}_1.1.0" % Test
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false
