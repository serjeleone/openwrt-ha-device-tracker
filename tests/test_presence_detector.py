"""Regression tests for Wi-Fi discovery, MQTT health and fallback recovery."""

import importlib.util
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
import subprocess

SOURCE = Path(__file__).resolve().parents[1] / "presence-detector.py"
SPEC = importlib.util.spec_from_file_location("presence_detector", SOURCE)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class PresenceDetectorRecoveryTests(unittest.TestCase):
    def test_auto_discovery_refreshes_interfaces(self):
        settings = MODULE.Settings.__new__(MODULE.Settings)
        settings._settings = {"interfaces": ["hostapd.phy0-ap0"]}
        settings.auto_detect_interfaces = True
        with patch.object(
            settings,
            "list_wifi_interfaces",
            return_value=["hostapd.phy0-ap0", "hostapd.phy1-ap0"],
        ):
            self.assertTrue(settings.refresh_interfaces())
            self.assertFalse(settings.refresh_interfaces())
        self.assertEqual(len(settings.interfaces), 2)

    def test_explicit_interfaces_are_not_overwritten(self):
        settings = MODULE.Settings.__new__(MODULE.Settings)
        settings._settings = {"interfaces": ["hostapd.custom"]}
        settings.auto_detect_interfaces = False
        with patch.object(settings, "list_wifi_interfaces") as discover:
            self.assertFalse(settings.refresh_interfaces())
            discover.assert_not_called()

    def test_incomplete_ubus_snapshot_cannot_mark_devices_away(self):
        detector = MODULE.PresenceDetector.__new__(MODULE.PresenceDetector)
        detector._settings = SimpleNamespace(
            interfaces=["hostapd.phy0-ap0", "hostapd.phy1-ap0"]
        )
        detector._logger = Mock()
        first = subprocess.CompletedProcess(
            args=["ubus"], returncode=0,
            stdout=json.dumps({"clients": {"34:39:16:71:a2:ac": {}}}),
            stderr="",
        )
        with patch.object(
            MODULE.subprocess,
            "run",
            side_effect=[first, subprocess.TimeoutExpired("ubus", 5)],
        ):
            self.assertIsNone(detector._get_all_online_devices())

    def test_health_requires_all_wifi_subscriptions(self):
        detector = MODULE.PresenceDetector.__new__(MODULE.PresenceDetector)
        detector._settings = SimpleNamespace(
            interfaces=["hostapd.phy0-ap0", "hostapd.phy1-ap0"]
        )
        detector._watchers = {
            "hostapd.phy0-ap0": SimpleNamespace(is_subscribed=True)
        }
        self.assertFalse(detector._watchers_healthy())
        detector._watchers["hostapd.phy1-ap0"] = SimpleNamespace(
            is_subscribed=True
        )
        self.assertTrue(detector._watchers_healthy())

    def test_fallback_sync_not_starved_by_heartbeat_messages(self):
        detector = MODULE.PresenceDetector.__new__(MODULE.PresenceDetector)
        detector._settings = SimpleNamespace(fallback_sync_interval=30)
        detector._logger = Mock()
        detector._killed = False
        detector._queue = Mock()
        detector._health_pending = Mock()
        detector._signal_wakeup = Mock()
        detector._do_full_sync = Mock()
        detector._refresh_watchers = Mock()
        detector.start_watchers = Mock()
        detector.start_signal_poller = Mock()
        detector.start_health_monitor = Mock()
        detector._publish_health = Mock(return_value=True)

        clock = [0.0]

        def get_message(timeout=None):
            del timeout
            clock[0] += 10
            if clock[0] >= 70:
                return MODULE.QueueItem(
                    "quit", "", MODULE.QueueItem.Action.QUIT
                )
            return MODULE.QueueItem(
                "health", "", MODULE.QueueItem.Action.HEALTH
            )

        detector._queue.get.side_effect = get_message
        with patch.object(
            MODULE.time, "monotonic", side_effect=lambda: clock[0]
        ):
            detector.run()
        self.assertEqual(detector._do_full_sync.call_count, 3)
        self.assertEqual(detector._refresh_watchers.call_count, 2)

    def _test_detector(self):
        """Build a detector with a single tracked client on the second radio."""
        detector = MODULE.PresenceDetector.__new__(MODULE.PresenceDetector)
        detector._settings = SimpleNamespace(
            auto_detect_interfaces=False,
            allow_list=["34:39:16:71:a2:ac"],
            interfaces=["hostapd.phy0-ap0", "hostapd.phy1-ap0"],
            params={"34:39:16:71:a2:ac": {}},
            location="home",
        )
        detector._logger = Mock()
        detector._clients_lock = MODULE.Lock()
        detector._queue = Mock()
        detector._signal_wakeup = Mock()
        detector._registered_clients = {"already_discovered"}
        detector._known_clients = {"34:39:16:71:a2:ac"}
        detector._last_seen_clients = {
            ("hostapd.phy1-ap0", "34:39:16:71:a2:ac")
        }
        detector._online_clients = {
            "hostapd.phy0-ap0": set(),
            "hostapd.phy1-ap0": {"34:39:16:71:a2:ac"},
        }
        detector._set_signal_availability = Mock()
        detector._get_all_online_devices = Mock(
            return_value=[("hostapd.phy1-ap0", "34:39:16:71:a2:ac")]
        )
        return detector

    def test_unchanged_fallback_does_not_publish_or_wake_rssi(self):
        detector = self._test_detector()
        for _ in range(3):
            detector._do_full_sync()
        detector._queue.put.assert_not_called()
        detector._signal_wakeup.set.assert_not_called()
        self.assertEqual(detector._registered_clients, {"already_discovered"})

    def test_fallback_detects_missing_assoc_event(self):
        detector = self._test_detector()
        detector._online_clients["hostapd.phy1-ap0"].clear()
        detector._last_seen_clients = set()
        detector._do_full_sync()
        item = detector._queue.put.call_args.args[0]
        self.assertEqual(item.action, MODULE.QueueItem.Action.ADD)
        self.assertEqual(item.device, "34:39:16:71:a2:ac")
        self.assertEqual(
            detector._online_clients["hostapd.phy1-ap0"],
            {"34:39:16:71:a2:ac"},
        )
        detector._signal_wakeup.set.assert_called_once()
        self.assertEqual(detector._registered_clients, {"already_discovered"})

    def test_fallback_detects_missing_disassoc_event(self):
        detector = self._test_detector()
        detector._get_all_online_devices.return_value = []
        detector._do_full_sync()
        item = detector._queue.put.call_args.args[0]
        self.assertEqual(item.action, MODULE.QueueItem.Action.DELETE)
        self.assertFalse(detector._online_clients["hostapd.phy1-ap0"])
        detector._set_signal_availability.assert_called_once_with(
            "34:39:16:71:a2:ac", False
        )

    def test_fallback_roaming_never_publishes_away(self):
        detector = self._test_detector()
        mac = "34:39:16:71:a2:ac"
        detector._online_clients = {
            "hostapd.phy0-ap0": {mac},
            "hostapd.phy1-ap0": set(),
        }
        detector._last_seen_clients = {("hostapd.phy0-ap0", mac)}
        detector._do_full_sync()
        items = [c.args[0] for c in detector._queue.put.call_args_list]
        self.assertEqual(
            [item.action for item in items],
            [MODULE.QueueItem.Action.ADD],
        )
        self.assertEqual(detector._online_clients["hostapd.phy1-ap0"], {mac})
        self.assertFalse(detector._online_clients["hostapd.phy0-ap0"])

    def test_explicit_ha_sync_republishes_home_and_away(self):
        detector = self._test_detector()
        away_mac = "b0:8c:75:1d:58:45"
        detector._settings.allow_list.append(away_mac)
        detector._settings.params[away_mac] = {}
        detector._known_clients.add(away_mac)
        detector._do_full_sync(republish_known=True)
        items = [c.args[0] for c in detector._queue.put.call_args_list]
        self.assertEqual(
            [item.action for item in items],
            [MODULE.QueueItem.Action.ADD, MODULE.QueueItem.Action.DELETE],
        )
        self.assertEqual(
            {item.device for item in items},
            {"34:39:16:71:a2:ac", away_mac},
        )
        detector._signal_wakeup.set.assert_called_once()

    def test_mqtt_connect_queues_explicit_resync(self):
        detector = self._test_detector()
        detector._mqtt = Mock()
        detector._registered_signal_clients = {"old_signal"}
        detector._health_registered = True
        detector._clear_signal_publish_cache = Mock()
        detector._queue_health_check = Mock()
        detector._on_mqtt_connect(None, None, None, 0)
        detector._mqtt.subscribe.assert_called_once_with("homeassistant/status")
        self.assertFalse(detector._registered_clients)
        self.assertFalse(detector._registered_signal_clients)
        self.assertFalse(detector._health_registered)
        detector._clear_signal_publish_cache.assert_called_once()
        detector._signal_wakeup.set.assert_called_once()
        item = detector._queue.put.call_args.args[0]
        self.assertEqual(item.action, MODULE.QueueItem.Action.SYNC)

    def test_ubus_watcher_resubscribes_after_eof(self):
        join = Mock()
        leave = Mock()
        logger = Mock()
        watcher = MODULE.UbusWatcher(
            "hostapd.phy1-ap0", join, leave, logger
        )

        process1 = Mock()
        process1.poll.return_value = None
        process1.stdout.readline.side_effect = [""]
        process2 = Mock()
        process2.poll.return_value = None
        process2.stdout.readline.side_effect = [
            '{"assoc": {"address": "34:39:16:71:A2:AC"}}\n',
            "",
        ]
        waits = [0]

        def backoff(_seconds):
            waits[0] += 1
            if waits[0] >= 2:
                watcher._stop_event.set()
            return watcher._stop_event.is_set()

        watcher._stop_event.wait = backoff
        with patch.object(
            MODULE.subprocess, "Popen", side_effect=[process1, process2]
        ) as popen:
            watcher.run()
        self.assertEqual(popen.call_count, 2)
        join.assert_called_once_with(
            "hostapd.phy1-ap0", "34:39:16:71:a2:ac"
        )
        leave.assert_not_called()


if __name__ == "__main__":
    unittest.main()
