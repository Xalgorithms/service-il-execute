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
package org.xalgorithms.actors

import akka.actor._
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.{ Producer }
import akka.stream.{ ActorMaterializer, OverflowStrategy }
import akka.stream.scaladsl._
import javax.inject._
import java.io.{ ByteArrayOutputStream, OutputStream }
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import scala.util.Properties

import org.xalgorithms.streams.AkkaStreams

trait Serializer[T] {
  def write(t: T): String
}

trait QueueForKafka[T] extends AkkaStreams {
  implicit val topic: String
  implicit val serializer: Serializer[T]

  private val _source = Source.queue[T](5, OverflowStrategy.backpressure)
  private val _flow_serialize = Flow[T].map(serializer.write(_))
  private val _flow_record = Flow[String].map { s => new ProducerRecord[String, String](topic, s) }

  var _q: SourceQueue[T] = null

  def send(t: T): Unit = {
    if (_q == null) {
      val settings = ProducerSettings(
        actor_system, new StringSerializer, new StringSerializer
      ).withBootstrapServers(Properties.envOrElse("KAFKA_BROKER", "kafka:9092"))
      _q = _source.via(_flow_serialize).via(_flow_record).to(Producer.plainSink(settings)).run()
    }

    _q.offer(t)
  }
}
