// Copyright (C) 2018 Don Kelly <karfai@gmail.com>

// This file is part of Interlibr, a functional component of an
// Internet of Rules (IoR).

// ACKNOWLEDGEMENTS
// Funds: Xalgorithms Foundation
// Collaborators: Don Kelly, Joseph Potvin and Bill Olders.

// This program is free software: you can redistribute it and/or
// modify it under the terms of the GNU Affero General Public License
// as published by the Free Software Foundation, either version 3 of
// the License, or (at your option) any later version.

// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
// Affero General Public License for more details.

// You should have received a copy of the GNU Affero General Public
// License along with this program. If not, see
// <http://www.gnu.org/licenses/>.
lazy val VERSION_MONGO_SCALA       = "2.3.0"
lazy val VERSION_SCALA             = "2.12.4"
lazy val VERSION_SCALA_TEST        = "3.1.2"
lazy val VERSION_CASSANDRA         = "3.5.0"
lazy val VERSION_AKKA_STREAM_KAFKA = "0.20"
lazy val VERSION_SCALA_MOCK        = "4.1.0"
lazy val VERSION_JODA              = "2.10"
lazy val VERSION_JODA_CONVERT      = "2.1"
// for easy hexdigest generation
lazy val VERSION_PLAY              = "2.6.0"

// ours
lazy val VERSION_STORAGE           = "0.0.6"
lazy val VERSION_RULES_INTERPRETER = "0.0.5"

lazy val meta = Seq(
  name := """service-il-execute""",
  organization := "org.xalgorithms",
  version := "0.0.6",
  scalaVersion := VERSION_SCALA,
)

lazy val lib_deps = Seq(
  // ours
  "org.xalgorithms"        %% "il-storage"              % VERSION_STORAGE from s"https://github.com/Xalgorithms/lib-storage/releases/download/v${VERSION_STORAGE}/il-storage_2.12-${VERSION_STORAGE}.jar",
  "org.xalgorithms"        %% "il-rules-interpreter"    % VERSION_RULES_INTERPRETER from s"https://github.com/Xalgorithms/lib-rules-int-scala/releases/download/v${VERSION_RULES_INTERPRETER}/il-rules-interpreter_2.12-${VERSION_RULES_INTERPRETER}.jar",
  // outer
  "org.mongodb.scala"      %% "mongo-scala-driver"      % VERSION_MONGO_SCALA,
  "com.typesafe.akka"      %% "akka-stream-kafka"       % VERSION_AKKA_STREAM_KAFKA,
  "com.typesafe.play"      %% "play"                    % VERSION_PLAY,
  "com.datastax.cassandra" %  "cassandra-driver-core"   % VERSION_CASSANDRA,
  "joda-time"              %  "joda-time"               % VERSION_JODA,
  "org.joda"               %  "joda-convert"            % VERSION_JODA_CONVERT,
  "org.scalamock"          %% "scalamock"               % VERSION_SCALA_MOCK % Test
)

lazy val root = (project in file("."))
  .settings(meta)
  .settings(libraryDependencies ++= lib_deps)
  .enablePlugins(JavaAppPackaging)
  .enablePlugins(DockerPlugin)
  .enablePlugins(AshScriptPlugin)

dockerBaseImage := "openjdk:jre-alpine"
dockerUsername := Some("xalgorithms")
