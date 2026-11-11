# This file is part of ctrl_filterd
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import json
import logging

from confluent_kafka import KafkaError, KafkaException
from rucio.client import Client

LOGGER = logging.getLogger(__name__)


class MessageFilter:
    """Consume Kafka messages, filter the ones marked "transfer-done", and
    forward them using "<dst-rse>-<scope>" as the Kafka topic.

    Parameters
    ----------
    consumer : `confluent_kafka.Consumer`
        Kafka consumer used to receive messages.
    producer : `confluent_kafka.Producer`
        Kafka producer used to send messages.
    topics : `list` [`str`]
        Destination topics that messages may be forwarded to.
    """

    RUBIN_BUTLER = "rubin_butler"
    RUBIN_SIDECAR = "rubin_sidecar"

    def __init__(self, consumer, producer, topics):
        self.consumer = consumer
        self.producer = producer
        self.topics = topics
        self.client = Client()

        LOGGER.info("Messages will only be sent to kafka topics: %s", self.topics)

    @staticmethod
    def _parse(msg):
        try:
            message = json.loads(msg.value())
        except (TypeError, ValueError) as e:
            LOGGER.warning(
                "discarding non-JSON message at %s [%s] @ %s: %s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                e,
            )
            return None
        if not isinstance(message, dict) or not isinstance(message.get("event_type"), str):
            LOGGER.warning(
                "discarding message without event_type at %s [%s] @ %s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )
            return None
        return message

    def _forward(self, message):
        payload = message["payload"]
        if not isinstance(payload, dict):
            LOGGER.warning("transfer-done message has no payload: %s", message)
            return False

        dst_rse = payload.get("dst-rse")
        scope = payload.get("scope")
        name = payload.get("name")

        if not (dst_rse and scope and name):
            LOGGER.warning("transfer-done payload missing dst_rse/scope/name: %s", payload)
            return False

        destination = f"{dst_rse}-{scope}"
        if destination not in self.topics:
            return False

        metadata = self.client.get_metadata(plugin="ALL", scope=scope, name=name)
        LOGGER.debug("name and metadata: %s : %s", name, metadata)

        # check to see if this is intended to be ingested
        # if not, discard it
        butler_ingest = metadata.get(self.RUBIN_BUTLER)
        if butler_ingest is None:
            return False
        payload[self.RUBIN_BUTLER] = butler_ingest

        # check to see if there's sidecar metadata, and if there is,
        # include it.
        butler_sidecar = metadata.get(self.RUBIN_SIDECAR)
        if butler_sidecar is not None:
            payload[self.RUBIN_SIDECAR] = butler_sidecar

        # send the message to Kafka
        self.send_message(topic=destination, message=message)

        return True

    def process(self, num_messages, timeout):
        """Process incoming messages.  Waits for num_messages to arrive or
        until timeout seconds have passed, whichever comes first

        Parameters
        ----------
        num_messages: `int`
            Number of messages to receive before returning
        timeout: `float`
            Time in seconds to wait until returning
        """

        # read up to self.num_messages, with a timeout of self.timeout
        messages = self.consumer.consume(num_messages=num_messages, timeout=timeout)

        msg_count = 0
        discard_count = 0
        # cycle through all the messages, applying the filter rules
        # stated above.
        for msg in messages:
            err = msg.error()
            if err is not None:
                if err.fatal():
                    raise KafkaException(err)
                if err.code() != KafkaError._PARTITION_EOF:
                    LOGGER.warning("consumer error on %s: %s", msg.topic(), err)
                continue

            message = self._parse(msg)

            if message is None:
                discard_count += 1
                continue

            if message["event_type"].lower() != "transfer-done":
                discard_count += 1
                continue

            try:
                if self._forward(message):
                    msg_count += 1
                else:
                    discard_count += 1
            except Exception:
                LOGGER.exception(
                    "error forwarding message from %s [%s] @ %s", msg.topic(), msg.partition(), msg.offset()
                )

        if msg_count:
            LOGGER.debug("sent %d messages to Kafka", msg_count)

        if discard_count:
            LOGGER.debug("discarded %d filtered messages", discard_count)

    def send_message(self, topic, message):
        """Send message to topic

        Parameters
        ----------
        topic: `str`
            Kafka topic to send message to
        message: `dict`
            Dictionary containing message values
        """
        d = {
            "event_type": str(message["event_type"]).lower(),
            "payload": message["payload"],
            "created_at": str(message["created_at"]),
        }

        LOGGER.debug("kafka sending: %s", message)
        value = json.dumps(d)
        self.producer.produce(topic, key="message", value=value)
        self.producer.flush()
