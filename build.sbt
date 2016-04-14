name := "BigData MIT"

version := "1.0"

scalaVersion := "2.10.4"

//libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.1"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.1.1"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

libraryDependencies += "com.xeiam.xchart" % "xchart" % "2.4.2"

