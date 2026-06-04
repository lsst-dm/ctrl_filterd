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

import os.path

import lsst.utils.tests
from lsst.ctrl.filterd.config import Config


class ConfigTestCase(lsst.utils.tests.TestCase):
    def createConfig(self, config_name):
        testdir = os.path.abspath(os.path.dirname(__file__))

        config_file = os.path.join(testdir, "data", config_name)
        self.config = Config.load(config_file)

    def testNoRses(self):
        with self.assertRaises(RuntimeError):
            self.createConfig("notopics.yml")

    def testNoBrokers(self):
        with self.assertRaises(RuntimeError):
            self.createConfig("nobrokers.yml")

    def testAttributes(self):
        self.createConfig("filterd.yml")

        self.assertEqual(",".join(self.config.brokers), "kafka:9092")
        self.assertEqual(self.config.consumer_client_id, "filterd_consumer")
        self.assertEqual(self.config.producer_client_id, "filterd_producer")
        self.assertEqual(self.config.group_id, "filterd_groupid")
        self.assertEqual(self.config.num_messages, 50)
        self.assertEqual(self.config.timeout, 1)
        self.assertEqual(self.config.topic, "hermestopic")

        topic_dict = self.config.rse_topics
        self.assertTrue("XRD1-test" in topic_dict)
        self.assertTrue("XRD2-test" in topic_dict)
        self.assertTrue("XRD3-test" in topic_dict)
        self.assertTrue("XRD4-test" in topic_dict)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
