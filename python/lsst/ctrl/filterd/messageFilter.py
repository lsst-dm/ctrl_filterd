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

from rucio.client import Client

LOGGER = logging.getLogger(__name__)


class MessageFilter:
    RUBIN_BUTLER = "rubin_butler"
    RUBIN_SIDECAR = "rubin_sidecar"

    """Consume Kafka messages, filter the ones marked "transfer-done", and
    forward them using "dst-rse" as the Kafka topic

    Parameters
    ----------
    consumer: `confluent.kafka.Consumer`
        Kafka consumer used to recieve messages
    producer: `confluent.kafka.Producer`
        Kafka producer used to send messages
    topics: `list[str]`
        This of candidate topics
    """

    def __init__(self, consumer, producer, topics):
        self.consumer = consumer
        self.producer = producer
        self.topics = topics
        self.client = Client()

        LOGGER.info(f'Messages will only be sent to kafka topics: "{self.topics}"')

    def process(self, num_messages, timeout):
        """Process incoming messages.  Waits for num_messages to arrive or
        until timeout seconds have passed, whichever comes first

        Parameters
        ----------
        num_messages: `int`
            Number of messages to receive before returning
        timeout: `int`
            Time is seconds to wait until returning
        """

        # read up to self.num_messages, with a timeout of self.timeout
        messages = self.consumer.consume(num_messages=num_messages, timeout=timeout)

        msg_count = 0
        discard_count = 0
        # cycle through all the messages, applying the filter rules
        # stated above.
        for msg in messages:
            # if the event_type isn't 'transfer-done', then ignore this message
            # and mark it for deletion.
            if msg.error():
                raise ValueError(msg.value())
            message = json.loads(msg.value())

            if str(message["event_type"]).lower() != "transfer-done":
                discard_count += 1
                continue
            try:
                # get the destination RSE
                dst_rse = str(message["payload"].get("dst-rse"))
                scope = str(message["payload"].get("scope"))

                # create Kakfa topic
                destination = f"{dst_rse}-{scope}"

                # check to see if the destination RSE is in the list
                # of topics we're filtering.  If it's not in the list
                # then discard it and go on to the next messages
                if self.topics is not None:
                    if destination not in self.topics:
                        # this destination RSE wasn't in the list specified
                        # in rucio.cfg, so discard it and go to the next
                        # message
                        discard_count += 1
                        continue

                # get the metadata for this file
                name = str(message["payload"].get("name"))
                metadata = self.client.get_metadata(plugin="ALL", scope=scope, name=name)
                LOGGER.debug("name and metadata: %s : %s", name, metadata)

                # check to see if this is a intended to be ingested
                # if not, discard it
                butler_ingest = metadata.get(self.RUBIN_BUTLER)
                if butler_ingest is None:
                    discard_count += 1
                    continue
                message["payload"][self.RUBIN_BUTLER] = butler_ingest

                # check to see if there's sidecar metadata, and if there is,
                # include it.
                butler_sidecar = metadata.get(self.RUBIN_SIDECAR)
                if butler_sidecar is not None:
                    message["payload"][self.RUBIN_SIDECAR] = butler_sidecar

                # send the message to Kafka
                self.send_message(topic=destination, message=message)

                msg_count += 1
            except Exception as exc:
                LOGGER.warning(f"error sending {message}: {exc}")

        if msg_count > 0:
            LOGGER.debug("sent %d messages to Kafka", msg_count)
        if discard_count > 0:
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

        LOGGER.debug(f"kafka sending: {message}")
        value = json.dumps(d)
        self.producer.produce(topic, key="message", value=value)
        self.producer.flush()
