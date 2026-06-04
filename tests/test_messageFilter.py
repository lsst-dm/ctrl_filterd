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

"""Unit tests for lsst.ctrl.filterd.messageFilter.MessageFilter.

The MessageFilter class depends on ``rucio.client.Client`` and on
confluent-kafka consumer/producer objects.  Both are fully mocked so
the tests require neither package to be installed.
"""

# Ensure ``rucio.client`` is importable before any lsst imports that
# depend on it.  If the real rucio package is already installed this
# block is a no-op.  The actual Client class is replaced per-test via
# ``unittest.mock.patch.object``.
import sys
import types

if "rucio" not in sys.modules:
    sys.modules["rucio"] = types.ModuleType("rucio")
if "rucio.client" not in sys.modules:
    _mod = types.ModuleType("rucio.client")
    _mod.Client = None
    sys.modules["rucio.client"] = _mod
    sys.modules["rucio"].client = _mod

import json
import unittest
from unittest.mock import MagicMock, patch

from lsst.ctrl.filterd import messageFilter
from lsst.ctrl.filterd.messageFilter import MessageFilter

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


class _KafkaMsg:
    """Minimal stand-in for a confluent_kafka Message."""

    def __init__(self, payload, *, err=None):
        if isinstance(payload, bytes):
            self._raw = payload
        else:
            self._raw = json.dumps(payload).encode()
        self._err = err

    def value(self):
        return self._raw

    def error(self):
        return self._err


def _transfer_done(
    dst_rse="USDF",
    scope="test_scope",
    name="file.fits",
    created_at="2025-01-15T12:00:00",
):
    """Return a dict representing a Rucio ``transfer-done`` event."""
    return {
        "event_type": "transfer-done",
        "payload": {"dst-rse": dst_rse, "scope": scope, "name": name},
        "created_at": created_at,
    }


# ------------------------------------------------------------------
# Test suite
# ------------------------------------------------------------------


class TestMessageFilter(unittest.TestCase):
    def setUp(self):
        self.consumer = MagicMock()
        self.producer = MagicMock()
        self.topics = ["USDF-test_scope"]

        # Replace the ``Client`` name inside the messageFilter module
        # with a fresh mock for every test.  This works whether rucio
        # is real or stubbed via sys.modules.
        patcher = patch.object(messageFilter, "Client")
        self.addCleanup(patcher.stop)
        self.MockClientCls = patcher.start()

        # ``Client()`` returns this mock instance; tests configure its
        # ``get_metadata`` return values as needed.
        self.rucio = MagicMock(name="rucio_client_instance")
        self.MockClientCls.return_value = self.rucio

        self.filter = MessageFilter(
            self.consumer,
            self.producer,
            self.topics,
        )

    # ---- helpers used by several tests ----

    def _process_one(self, msg_dict, filter=None):
        """Feed a single message through process()."""
        filter = filter or self.filter
        self.consumer.consume.return_value = [_KafkaMsg(msg_dict)]
        filter.process(num_messages=10, timeout=1)

    def _set_metadata(self, meta):
        """Configure the mock rucio client to return *meta*."""
        self.rucio.get_metadata.return_value = meta

    def _produced_value(self):
        """Deserialise the JSON value passed to producer.produce()."""
        return json.loads(self.producer.produce.call_args[1]["value"])

    # ==================================================================
    # Construction
    # ==================================================================

    def test_stores_consumer(self):
        self.assertIs(self.filter.consumer, self.consumer)

    def test_stores_producer(self):
        self.assertIs(self.filter.producer, self.producer)

    def test_stores_topics(self):
        self.assertEqual(self.filter.topics, self.topics)

    def test_creates_rucio_client(self):
        self.MockClientCls.assert_called_once_with()
        self.assertIs(self.filter.client, self.rucio)

    def test_class_constants(self):
        self.assertEqual(MessageFilter.RUBIN_BUTLER, "rubin_butler")
        self.assertEqual(MessageFilter.RUBIN_SIDECAR, "rubin_sidecar")

    # ==================================================================
    # process() — consume forwarding
    # ==================================================================

    def test_forwards_consume_args(self):
        self.consumer.consume.return_value = []
        self.filter.process(num_messages=42, timeout=7)
        self.consumer.consume.assert_called_once_with(
            num_messages=42,
            timeout=7,
        )

    def test_empty_batch_is_noop(self):
        self.consumer.consume.return_value = []
        self.filter.process(num_messages=10, timeout=1)
        self.producer.produce.assert_not_called()

    # ==================================================================
    # process() — Kafka error on consumed message
    # ==================================================================

    def test_kafka_error_raises(self):
        err = MagicMock()
        err.__bool__ = MagicMock(return_value=True)
        msg = _KafkaMsg(b"boom", err=err)
        self.consumer.consume.return_value = [msg]
        with self.assertRaises(Exception) as ctx:
            self.filter.process(num_messages=10, timeout=1)
        self.assertEqual(ctx.exception.args[0], b"boom")

    # ==================================================================
    # process() — event_type filter
    # ==================================================================

    def test_discards_non_transfer_done_event(self):
        self._process_one({"event_type": "deletion-done", "payload": {}})
        self.producer.produce.assert_not_called()

    def test_event_type_check_is_case_insensitive(self):
        self._process_one({"event_type": "TRANSFER-STARTED", "payload": {}})
        self.producer.produce.assert_not_called()

    def test_accepts_mixed_case_transfer_done(self):
        msg = _transfer_done()
        msg["event_type"] = "Transfer-Done"
        self._set_metadata({"rubin_butler": True})
        self._process_one(msg)
        self.producer.produce.assert_called_once()

    # ==================================================================
    # process() — destination topic filter
    # ==================================================================

    def test_discards_when_destination_not_in_topics(self):
        msg = _transfer_done(dst_rse="OTHERSITE", scope="other")
        self._process_one(msg)
        self.producer.produce.assert_not_called()

    def test_skips_topic_filter_when_topics_is_none(self):
        filter = MessageFilter(self.consumer, self.producer, topics=None)
        self._set_metadata({"rubin_butler": True})
        self._process_one(
            _transfer_done(dst_rse="ANYWHERE", scope="any"),
            filter=filter,
        )
        self.producer.produce.assert_called_once()

    # ==================================================================
    # process() — rucio metadata filter
    # ==================================================================

    def test_discards_when_no_rubin_butler_metadata(self):
        self._set_metadata({})
        self._process_one(_transfer_done())
        self.producer.produce.assert_not_called()

    def test_discards_when_rubin_butler_is_none(self):
        self._set_metadata({"rubin_butler": None})
        self._process_one(_transfer_done())
        self.producer.produce.assert_not_called()

    def test_calls_get_metadata_correctly(self):
        self._set_metadata({"rubin_butler": True})
        self._process_one(
            _transfer_done(
                dst_rse="USDF",
                scope="test_scope",
                name="myfile.fits",
            ),
        )
        self.rucio.get_metadata.assert_called_once_with(
            plugin="ALL",
            scope="test_scope",
            name="myfile.fits",
        )

    # ==================================================================
    # process() — happy path: message forwarded
    # ==================================================================

    def test_produces_message_with_butler_metadata(self):
        butler = {"ingest": True, "dataset_type": "raw"}
        self._set_metadata({"rubin_butler": butler})
        self._process_one(_transfer_done())

        self.producer.produce.assert_called_once()
        self.producer.flush.assert_called_once()
        self.assertEqual(
            self._produced_value()["payload"]["rubin_butler"],
            butler,
        )

    def test_routes_to_correct_topic(self):
        self._set_metadata({"rubin_butler": True})
        self._process_one(
            _transfer_done(dst_rse="USDF", scope="test_scope"),
        )
        topic_arg = self.producer.produce.call_args[0][0]
        self.assertEqual(topic_arg, "USDF-test_scope")

    def test_includes_sidecar_when_present(self):
        sidecar = {"extra": "data"}
        self._set_metadata(
            {
                "rubin_butler": True,
                "rubin_sidecar": sidecar,
            }
        )
        self._process_one(_transfer_done())
        self.assertEqual(
            self._produced_value()["payload"]["rubin_sidecar"],
            sidecar,
        )

    def test_omits_sidecar_when_absent(self):
        self._set_metadata({"rubin_butler": True})
        self._process_one(_transfer_done())
        self.assertNotIn("rubin_sidecar", self._produced_value()["payload"])

    # ==================================================================
    # process() — error resilience
    # ==================================================================

    def test_rucio_error_skips_message_and_continues(self):
        bad = _transfer_done(name="bad.fits")
        good = _transfer_done(name="good.fits")
        self.consumer.consume.return_value = [
            _KafkaMsg(bad),
            _KafkaMsg(good),
        ]

        def side_effect(*, plugin, scope, name):
            if name == "bad.fits":
                raise RuntimeError("rucio down")
            return {"rubin_butler": True}

        self.rucio.get_metadata.side_effect = side_effect

        self.filter.process(num_messages=10, timeout=1)
        self.assertEqual(self.producer.produce.call_count, 1)

    # ==================================================================
    # process() — mixed batch
    # ==================================================================

    def test_mixed_batch_produces_only_valid(self):
        valid = _transfer_done(name="ok.fits")
        wrong_event = {"event_type": "deletion-done", "payload": {}}
        wrong_dest = _transfer_done(dst_rse="NOPE", scope="nope")
        no_butler = _transfer_done(name="nobutler.fits")

        self.consumer.consume.return_value = [
            _KafkaMsg(valid),
            _KafkaMsg(wrong_event),
            _KafkaMsg(wrong_dest),
            _KafkaMsg(no_butler),
        ]

        call_count = {"n": 0}

        def side_effect(*, plugin, scope, name):
            call_count["n"] += 1
            if name == "nobutler.fits":
                return {}
            return {"rubin_butler": True}

        self.rucio.get_metadata.side_effect = side_effect

        self.filter.process(num_messages=10, timeout=1)

        # Only "ok.fits" should be produced; "nobutler.fits" reached
        # get_metadata but was discarded.  "wrong_dest" was topic-
        # filtered before reaching get_metadata.
        self.assertEqual(self.producer.produce.call_count, 1)
        self.assertEqual(call_count["n"], 2)  # ok.fits + nobutler.fits

    # ==================================================================
    # send_message()
    # ==================================================================

    def test_send_message_json_structure(self):
        msg = _transfer_done()
        self.filter.send_message(topic="T", message=msg)

        self.producer.produce.assert_called_once()
        args, kwargs = self.producer.produce.call_args
        self.assertEqual(args[0], "T")
        self.assertEqual(kwargs["key"], "message")

        body = json.loads(kwargs["value"])
        self.assertEqual(
            set(body.keys()),
            {"event_type", "payload", "created_at"},
        )

    def test_send_message_flushes(self):
        self.filter.send_message(topic="T", message=_transfer_done())
        self.producer.flush.assert_called_once()


if __name__ == "__main__":
    unittest.main()
