name := "common-redis"

version := "1.0"

scalaVersion := "2.11.8"

organization := "com.lawsofnature.common"

// https://mvnrepository.com/artifact/redis.clients/jedis
libraryDependencies += "redis.clients" % "jedis" % "2.9.0"

libraryDependencies += "net.codingwell" %% "scala-guice" % "4.0.1"

libraryDependencies += "com.lawsofnature.common" % "common-utils_2.11" % "1.0"
libraryDependencies += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.3"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7"