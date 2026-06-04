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

import logging
import os

from confluent_kafka import Consumer, Producer

from lsst.ctrl.filterd.config import Config
from lsst.ctrl.filterd.messageFilter import MessageFilter

LOGGER = logging.getLogger(__name__)

CTRL_FILTERD_CONFIG = "CTRL_FILTERD_CONFIG"


class FilterD:
    """Entry point for filterd"""

    def __init__(self):
        if CTRL_FILTERD_CONFIG in os.environ:
            self.config_file = os.environ[CTRL_FILTERD_CONFIG]
        else:
            raise FileNotFoundError("CTRL_FILTERD_CONFIG is not set")

        config = Config.load(self.config_file)

        LOGGER.info("brokers = %s", ",".join(config.brokers))
        LOGGER.info("consumer_client_id = %s", config.consumer_client_id)
        LOGGER.info("group.id = %s", config.group_id)
        LOGGER.info("producer_client_id = %s", config.producer_client_id)
        LOGGER.info("num_messages = %d", config.num_messages)
        LOGGER.info("timeout = %d", config.timeout)
        LOGGER.info("topic = %s", config.topic)
        LOGGER.info("rse_topics = %s", ",".join(config.rse_topics))

        consumer_client_id = config.consumer_client_id
        producer_client_id = config.producer_client_id
        group_id = config.group_id
        brokers = ",".join(config.brokers)
        topic = config.topic
        rse_topics = config.rse_topics

        self.num_messages = config.num_messages
        self.timeout = config.timeout

        consumer_conf = {
            "bootstrap.servers": brokers,
            "client.id": consumer_client_id,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }

        consumer = Consumer(consumer_conf)
        consumer.subscribe([topic])

        producer_conf = {
            "bootstrap.servers": f"{brokers}",
            "client.id": producer_client_id,
        }
        producer = Producer(producer_conf)

        self.message_filter = MessageFilter(consumer, producer, rse_topics)

    def run(self):
        """continually process messages"""
        while True:
            self.message_filter.process(self.num_messages, self.timeout)


if __name__ == "__main__":
    filterd = FilterD()
    filterd.run()
