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

The Kafka consumer/producer and the Rucio client are all mocked, so
these tests need neither a broker nor a Rucio server.  ``rucio`` itself
need not be installed; ``confluent_kafka`` must be (it is part of
rubin-env).
"""

import json
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

from confluent_kafka import KafkaError, KafkaException

# messageFilter does ``from rucio.client import Client`` at import time.
# If rucio isn't installed, provide a placeholder module just long enough
# to import messageFilter; patch.dict removes it from sys.modules
# afterwards so it can't leak into other test modules.  The real Client
# is replaced per-test in setUp either way.
with patch.dict(sys.modules):
    if "rucio.client" not in sys.modules:
        try:
            import rucio.client  # noqa: F401
        except ImportError:
            _rucio = types.ModuleType("rucio")
            _rucio_client = types.ModuleType("rucio.client")
            _rucio_client.Client = None
            _rucio.client = _rucio_client
            sys.modules["rucio"] = _rucio
            sys.modules["rucio.client"] = _rucio_client
    from lsst.ctrl.filterd import messageFilter

MessageFilter = messageFilter.MessageFilter
LOGGER_NAME = messageFilter.LOGGER.name

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


class _KafkaMsg:
    """Minimal stand-in for a confluent_kafka Message."""

    def __init__(self, payload, *, err=None, topic="hermestopic", partition=0, offset=0):
        if isinstance(payload, bytes):
            self._raw = payload
        else:
            self._raw = json.dumps(payload).encode()
        self._err = err
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def value(self):
        return self._raw

    def error(self):
        return self._err

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


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
        # with a fresh mock for every test.
        patcher = patch.object(messageFilter, "Client")
        self.addCleanup(patcher.stop)
        self.MockClientCls = patcher.start()

        # ``Client()`` returns this mock instance; tests configure its
        # ``get_metadata`` return values as needed.
        self.rucio = MagicMock(name="rucio_client_instance")
        self.MockClientCls.return_value = self.rucio

        self.filter = MessageFilter(self.consumer, self.producer, self.topics)

    # ---- helpers used by several tests ----

    def _process(self, *msgs):
        """Feed messages (dicts, bytes, or _KafkaMsg) through process()."""
        self.consumer.consume.return_value = [m if isinstance(m, _KafkaMsg) else _KafkaMsg(m) for m in msgs]
        self.filter.process(num_messages=10, timeout=1)

    def _set_metadata(self, meta):
        """Configure the mock rucio client to return *meta*."""
        self.rucio.get_metadata.return_value = meta

    def _produced_value(self):
        """Deserialise the JSON value passed to producer.produce()."""
        return json.loads(self.producer.produce.call_args.kwargs["value"])

    def _produced_names(self):
        """Return the payload names of every produced message, in order."""
        calls = self.producer.produce.call_args_list
        return [json.loads(c.kwargs["value"])["payload"]["name"] for c in calls]

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

    def test_has_docstring(self):
        self.assertIsNotNone(MessageFilter.__doc__)

    # ==================================================================
    # process() — consume forwarding
    # ==================================================================

    def test_forwards_consume_args(self):
        self.consumer.consume.return_value = []
        self.filter.process(num_messages=42, timeout=7)
        self.consumer.consume.assert_called_once_with(num_messages=42, timeout=7)

    def test_empty_batch_is_noop(self):
        self._process()
        self.producer.produce.assert_not_called()

    # ==================================================================
    # process() — Kafka errors on consumed messages
    # ==================================================================

    def test_fatal_kafka_error_raises(self):
        err = KafkaError(KafkaError._FATAL, "broker gone", fatal=True)
        with self.assertRaises(KafkaException) as ctx:
            self._process(_KafkaMsg(b"", err=err))
        self.assertIs(ctx.exception.args[0], err)

    def test_non_fatal_kafka_error_is_logged_and_skipped(self):
        err = KafkaError(KafkaError._TRANSPORT, "connection reset")
        self._set_metadata({"rubin_butler": True})
        with self.assertLogs(LOGGER_NAME, level="WARNING") as logs:
            self._process(_KafkaMsg(b"", err=err), _transfer_done())
        self.assertIn("consumer error", logs.output[0])
        self.producer.produce.assert_called_once()  # later message still handled

    def test_partition_eof_is_skipped_silently(self):
        err = KafkaError(KafkaError._PARTITION_EOF)
        with self.assertNoLogs(LOGGER_NAME, level="WARNING"):
            self._process(_KafkaMsg(b"", err=err))
        self.producer.produce.assert_not_called()

    # ==================================================================
    # process() — malformed messages are discarded, never raised
    # ==================================================================

    def test_non_json_message_is_discarded(self):
        self._set_metadata({"rubin_butler": True})
        with self.assertLogs(LOGGER_NAME, level="WARNING") as logs:
            self._process(_KafkaMsg(b"not json", offset=17), _transfer_done())
        self.assertIn("non-JSON", logs.output[0])
        self.assertIn("@ 17", logs.output[0])
        self.producer.produce.assert_called_once()

    def test_non_dict_json_is_discarded(self):
        with self.assertLogs(LOGGER_NAME, level="WARNING"):
            self._process(b'["transfer-done"]')
        self.producer.produce.assert_not_called()

    def test_missing_event_type_is_discarded(self):
        with self.assertLogs(LOGGER_NAME, level="WARNING"):
            self._process({"payload": {}})
        self.producer.produce.assert_not_called()

    def test_non_string_event_type_is_discarded(self):
        with self.assertLogs(LOGGER_NAME, level="WARNING"):
            self._process({"event_type": None, "payload": {}})
        self.producer.produce.assert_not_called()

    def test_missing_payload_is_discarded(self):
        msg = _transfer_done()
        del msg["payload"]
        with self.assertLogs(LOGGER_NAME, level="WARNING"):
            self._process(msg)
        self.rucio.get_metadata.assert_not_called()
        self.producer.produce.assert_not_called()

    def test_missing_payload_field_is_discarded(self):
        for field in ("dst-rse", "scope", "name"):
            with self.subTest(field=field):
                self.rucio.reset_mock()
                self.producer.reset_mock()
                msg = _transfer_done()
                del msg["payload"][field]
                with self.assertLogs(LOGGER_NAME, level="WARNING"):
                    self._process(msg)
                self.rucio.get_metadata.assert_not_called()
                self.producer.produce.assert_not_called()

    # ==================================================================
    # process() — event_type filter
    # ==================================================================

    def test_discards_non_transfer_done_events(self):
        for event_type in ("deletion-done", "TRANSFER-STARTED", "transfer-failed"):
            with self.subTest(event_type=event_type):
                self._process({"event_type": event_type, "payload": {}})
                self.producer.produce.assert_not_called()

    def test_accepts_mixed_case_transfer_done(self):
        msg = _transfer_done()
        msg["event_type"] = "Transfer-Done"
        self._set_metadata({"rubin_butler": True})
        self._process(msg)
        self.producer.produce.assert_called_once()

    # ==================================================================
    # process() — destination topic filter
    # ==================================================================

    def test_discards_when_destination_not_in_topics(self):
        self._process(_transfer_done(dst_rse="OTHERSITE", scope="other"))
        self.rucio.get_metadata.assert_not_called()
        self.producer.produce.assert_not_called()

    # ==================================================================
    # process() — rucio metadata filter
    # ==================================================================

    def test_discards_when_no_rubin_butler_metadata(self):
        self._set_metadata({})
        self._process(_transfer_done())
        self.producer.produce.assert_not_called()

    def test_discards_when_rubin_butler_is_none(self):
        self._set_metadata({"rubin_butler": None})
        self._process(_transfer_done())
        self.producer.produce.assert_not_called()

    def test_calls_get_metadata_correctly(self):
        self._set_metadata({"rubin_butler": True})
        self._process(_transfer_done(dst_rse="USDF", scope="test_scope", name="myfile.fits"))
        self.rucio.get_metadata.assert_called_once_with(plugin="ALL", scope="test_scope", name="myfile.fits")

    # ==================================================================
    # process() — happy path: message forwarded
    # ==================================================================

    def test_produces_message_with_butler_metadata(self):
        butler = {"ingest": True, "dataset_type": "raw"}
        self._set_metadata({"rubin_butler": butler})
        self._process(_transfer_done())

        self.producer.produce.assert_called_once()
        self.producer.flush.assert_called_once()
        self.assertEqual(self._produced_value()["payload"]["rubin_butler"], butler)

    def test_routes_to_correct_topic(self):
        self._set_metadata({"rubin_butler": True})
        self._process(_transfer_done(dst_rse="USDF", scope="test_scope"))
        self.assertEqual(self.producer.produce.call_args.args[0], "USDF-test_scope")

    def test_includes_sidecar_when_present(self):
        sidecar = {"extra": "data"}
        self._set_metadata({"rubin_butler": True, "rubin_sidecar": sidecar})
        self._process(_transfer_done())
        self.assertEqual(self._produced_value()["payload"]["rubin_sidecar"], sidecar)

    def test_omits_sidecar_when_absent(self):
        self._set_metadata({"rubin_butler": True})
        self._process(_transfer_done())
        self.assertNotIn("rubin_sidecar", self._produced_value()["payload"])

    # ==================================================================
    # process() — error resilience
    # ==================================================================

    def test_rucio_error_is_logged_and_batch_continues(self):
        def side_effect(*, plugin, scope, name):
            if name == "bad.fits":
                raise RuntimeError("rucio down")
            return {"rubin_butler": True}

        self.rucio.get_metadata.side_effect = side_effect

        with self.assertLogs(LOGGER_NAME, level="ERROR") as logs:
            self._process(_transfer_done(name="bad.fits"), _transfer_done(name="good.fits"))

        self.assertEqual(self._produced_names(), ["good.fits"])
        # LOGGER.exception attaches the traceback
        self.assertIsNotNone(logs.records[0].exc_info)

    def test_produce_error_is_logged_and_batch_continues(self):
        self._set_metadata({"rubin_butler": True})
        self.producer.produce.side_effect = [BufferError("queue full"), None]

        with self.assertLogs(LOGGER_NAME, level="ERROR"):
            self._process(_transfer_done(name="first.fits"), _transfer_done(name="second.fits"))

        self.assertEqual(self.producer.produce.call_count, 2)

    # ==================================================================
    # process() — mixed batch
    # ==================================================================

    def test_mixed_batch_produces_only_valid(self):
        def side_effect(*, plugin, scope, name):
            if name == "nobutler.fits":
                return {}
            return {"rubin_butler": True}

        self.rucio.get_metadata.side_effect = side_effect

        with self.assertLogs(LOGGER_NAME, level="WARNING"):
            self._process(
                _transfer_done(name="ok.fits"),
                {"event_type": "deletion-done", "payload": {}},
                b"not json",
                _transfer_done(dst_rse="NOPE", scope="nope"),
                _transfer_done(name="nobutler.fits"),
                _transfer_done(name="also_ok.fits"),
            )

        self.assertEqual(self._produced_names(), ["ok.fits", "also_ok.fits"])
        # Only messages passing the event-type and topic filters reach Rucio.
        self.assertEqual(self.rucio.get_metadata.call_count, 3)

    # ==================================================================
    # send_message()
    # ==================================================================

    def test_send_message_json_structure(self):
        self.filter.send_message(topic="T", message=_transfer_done())

        self.producer.produce.assert_called_once()
        call = self.producer.produce.call_args
        self.assertEqual(call.args[0], "T")
        self.assertEqual(call.kwargs["key"], "message")
        body = json.loads(call.kwargs["value"])
        self.assertEqual(set(body), {"event_type", "payload", "created_at"})

    def test_send_message_lowercases_event_type(self):
        msg = _transfer_done()
        msg["event_type"] = "TRANSFER-DONE"
        self.filter.send_message(topic="T", message=msg)
        self.assertEqual(self._produced_value()["event_type"], "transfer-done")

    def test_send_message_flushes(self):
        self.filter.send_message(topic="T", message=_transfer_done())
        self.producer.flush.assert_called_once()


if __name__ == "__main__":
    unittest.main()
