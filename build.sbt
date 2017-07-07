name := "common-redis"

version := "1.1"

scalaVersion := "2.12.2"

organization := "com.jxjxgo.common"

// https://mvnrepository.com/artifact/redis.clients/jedis
libraryDependencies += "redis.clients" % "jedis" % "2.9.0"
libraryDependencies += "net.codingwell" %% "scala-guice" % "4.1.0"
libraryDependencies += "com.jxjxgo.common" % "common-utils_2.12" % "1.2"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7"