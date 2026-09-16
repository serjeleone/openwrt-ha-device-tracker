#!/usr/bin/env python3
# pylint: disable=too-few-public-methods,invalid-name,too-many-instance-attributes

"""
A Wi-Fi device presence detector for Home Assistant that runs on OpenWRT.

Presence is event-driven through hostapd ubus assoc/disassoc events. Wi-Fi signal
telemetry is read locally from iw station dumps and pushed to Home Assistant through
MQTT Discovery.
"""

import argparse
import json
import queue
import signal
import subprocess
import syslog
import time
from dataclasses import dataclass
from enum import IntEnum
from queue import Queue
from threading import Event, Lock, Thread
from typing import Any, Callable

from paho.mqtt import client as mqtt


class Logger:
    """Class to handle logging to syslog."""

    def __init__(self, enable_debug: bool) -> None:
        self.enable_debug = enable_debug

    def log(self, text: str, is_debug: bool = False) -> None:
        """Log a line to syslog. Only log debug messages when debugging is enabled."""
        if is_debug and not self.enable_debug:
            return

        level = syslog.LOG_DEBUG if is_debug else syslog.LOG_INFO
        syslog.openlog(
            ident="presence-detector",
            facility=syslog.LOG_DAEMON,
            logoption=syslog.LOG_PID,
        )
        syslog.syslog(level, text)


class Settings:
    """Loads all settings from a JSON file and provides built-in defaults."""

    def __init__(self, config_file: str) -> None:
        self._settings = {
            "mqtt_host": "192.168.1.50",
            "mqtt_port": 1883,
            "mqtt_username": "ha",
            "mqtt_password": "",
            "mqtt_retain_state": True,
            "interfaces": [],
            "allow_list": [],
            "params": {},
            "location": "home",
            "away": "not_home",
            "fallback_sync_interval": 60,
            "signal_poll_interval": 2,
            "source_type": "router",
            "debug": False,
        }
        with open(config_file, "r", encoding="utf-8") as settings:
            self._settings.update(json.load(settings))

        # Lowercase all MAC addresses in the allow list and params settings.
        self._settings["allow_list"] = [
            device.lower() for device in self.allow_list
        ]
        self._settings["params"] = {
            device.lower(): params for device, params in self.params.items()
        }

        # 0 disables signal polling; otherwise require a positive interval.
        try:
            self._settings["signal_poll_interval"] = float(
                self._settings["signal_poll_interval"]
            )
        except (TypeError, ValueError):
            self._settings["signal_poll_interval"] = 2.0
        if self._settings["signal_poll_interval"] < 0:
            self._settings["signal_poll_interval"] = 0.0

        if not self._settings["interfaces"]:
            self._settings["interfaces"] = self.list_wifi_interfaces()

    def __getattr__(self, item: str) -> Any:
        return self._settings.get(item)

    def list_wifi_interfaces(self) -> list[str]:
        """List all hostapd Wi-Fi ubus interfaces."""
        output = subprocess.run(
            ["ubus", "list", "hostapd.*"], stdout=subprocess.PIPE, check=True
        )
        return output.stdout.decode("utf-8").strip().split("\n")

    @staticmethod
    def deep_merge(dict1: dict, dict2: dict):
        """Deep merge two dictionaries."""
        result = dict1.copy()
        for key, value in dict2.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = Settings.deep_merge(result[key], value)
            else:
                result[key] = value
        return result


@dataclass
class QueueItem:
    """Represents a device item on the queue."""

    class Action(IntEnum):
        """Possible queue item actions."""

        ADD = 1
        DELETE = 2
        QUIT = 3
        SYNC = 4

    device: str
    interface: str
    action: Action


@dataclass
class SignalSample:
    """Wi-Fi signal sample for one associated station."""

    interface: str
    signal: int | None
    signal_avg: int | None

    @property
    def strength(self) -> int:
        """Value used when choosing the strongest AP during roaming."""
        if self.signal_avg is not None:
            return self.signal_avg
        if self.signal is not None:
            return self.signal
        return -999


class PresenceDetector(Thread):
    """Presence detector using ubus events plus local iw signal polling."""

    def __init__(self, config_file: str) -> None:
        super().__init__()
        self._settings = Settings(config_file)
        self._logger = Logger(self._settings.debug)
        self._queue: Queue = Queue()
        self._watchers: list[UbusWatcher] = []
        self._killed = False
        self._last_seen_clients: set[tuple[str, str]] | None = None
        self._online_clients: dict[str, set[str]] = {}
        self._registered_clients: set[str] = set()
        self._registered_signal_clients: set[str] = set()
        # Keep track of devices already seen by this process so Home Assistant
        # Discovery can be rebuilt after HA restarts even when a device is away.
        self._known_clients: set[str] = set(self._settings.params.keys())
        self._last_published_signal_values: dict[tuple[str, str], int] = {}
        self._last_published_signal_availability: dict[tuple[str, str], bool] = {}
        self._clients_lock = Lock()
        self._signal_stop = Event()
        self._signal_wakeup = Event()
        self._signal_thread: Thread | None = None
        for interface in self._settings.interfaces:
            self._online_clients[interface] = set()
        self._connect_to_mqtt()

    def _connect_to_mqtt(self):
        if hasattr(mqtt, "CallbackAPIVersion"):
            self._mqtt = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2
            )
        else:
            # Version 1 is deprecated but still supported.
            self._mqtt = mqtt.Client()
        self._mqtt.on_connect = self._on_mqtt_connect
        self._mqtt.on_disconnect = self._on_mqtt_disconnect
        if hasattr(self._mqtt, "on_connect_fail"):
            self._mqtt.on_connect_fail = self._on_mqtt_connect_fail
        self._mqtt.username_pw_set(
            self._settings.mqtt_username, self._settings.mqtt_password
        )
        self._mqtt.reconnect_delay_set(min_delay=1, max_delay=60)
        self._mqtt.message_callback_add(
            "homeassistant/status", self._on_ha_status_message
        )
        self._mqtt.connect_async(
            self._settings.mqtt_host, self._settings.mqtt_port, keepalive=60
        )
        self._mqtt.loop_start()

    def _on_mqtt_connect(
        self, _client, _userdata, _flags, reason_code, _properties=None
    ):
        """Callback for MQTT connection (supports both v1 and v2 API)."""
        is_failure = (
            reason_code.is_failure
            if hasattr(reason_code, "is_failure")
            else reason_code != 0
        )
        if is_failure:
            self._logger.log(f"MQTT broker connection failed (rc: {reason_code})")
            return
        self._logger.log("MQTT broker connected")
        self._mqtt.subscribe("homeassistant/status")
        self._signal_wakeup.set()

    def _on_mqtt_connect_fail(self, _client, _userdata):
        """Callback for MQTT connection failures."""
        self._logger.log("MQTT broker connection failed, retrying...")

    def _on_mqtt_disconnect(self, *args, **_kwargs):
        """Callback for MQTT disconnections (supports both v1 and v2 API)."""
        reason_code = args[3] if len(args) >= 4 else (args[2] if len(args) >= 3 else 0)
        self._logger.log(f"MQTT broker disconnected (rc: {reason_code})")
        self._registered_clients.clear()
        self._registered_signal_clients.clear()
        self._clear_signal_publish_cache()

    def _on_ha_status_message(self, _client, _userdata, message):
        """Callback for HA status messages."""
        if message.payload == b"offline":
            self._logger.log("Home Assistant is offline!")
            self._registered_clients.clear()
            self._registered_signal_clients.clear()
            self._clear_signal_publish_cache()
        elif message.payload == b"online":
            self._logger.log("Home Assistant is back online")
            # Discovery messages are not retained. They may have been published
            # while HA was offline, so force both tracker and RSSI discovery to
            # be registered again. Do the actual sync in the detector thread,
            # not inside the Paho callback thread.
            self._registered_clients.clear()
            self._registered_signal_clients.clear()
            self._clear_signal_publish_cache()
            self._queue.put(QueueItem("sync", "", QueueItem.Action.SYNC))

    def _publish(self, topic: str, data: str, retain=False) -> bool:
        self._logger.log(f"Publishing to {topic}: {data}", True)
        if not self._mqtt.is_connected():
            return False
        result = self._mqtt.publish(topic, data, qos=1, retain=retain)
        try:
            result.wait_for_publish(timeout=5)
        except (RuntimeError, ValueError) as ex:
            self._logger.log(f"Error publishing to {topic}: {ex}", False)
            return False
        return result.is_published()

    def _device_slug(self, device: str) -> str:
        """Return the stable MQTT/HA slug used for a device."""
        device_slug = device.replace(":", "_")
        if self._settings.ap_name:
            device_slug = f"{self._settings.ap_name}_{device_slug}"
        return device_slug

    def _device_info(self, device: str) -> dict:
        """Build HA MQTT device metadata shared by tracker and signal sensors."""
        device_info = {"connections": [["mac", device]]}
        params = self._settings.params.get(device, {})
        if isinstance(params.get("device"), dict):
            device_info = Settings.deep_merge(device_info, params["device"])
        if "name" not in device_info and params.get("name"):
            device_info["name"] = params["name"]
        device_info["manufacturer"] = "OpenWrt"
        device_info["model"] = "Device tracker"
        return device_info

    def _ha_seen(self, device: str, seen: bool = True) -> bool:
        """Publish MQTT messages to register and update home/away status."""
        device_slug = self._device_slug(device)
        device_name = device.replace(":", "_")

        ok = True
        if device_slug not in self._registered_clients:
            self._registered_clients.add(device_slug)
            body = {
                "state_topic": f"homeassistant/device_tracker/{device_slug}/state",
                "json_attributes_topic": f"homeassistant/device_tracker/{device_slug}/state",
                "value_template": "{{ value_json['state'] }}",
                "name": device_name,
                "platform": "device_tracker",
                "payload_home": self._settings.location,
                "payload_not_home": self._settings.away,
                "source_type": self._settings.source_type,
                "device": {"connections": [["mac", device]]},
                "unique_id": device_slug,
            }
            if device in self._settings.params:
                body = Settings.deep_merge(body, self._settings.params[device])
                body["device"]["manufacturer"] = "OpenWrt"
                body["device"]["model"] = "Device tracker"
                if "name" not in body["device"] and body.get("name"):
                    body["device"]["name"] = body["name"]
                # When a configured name becomes the Home Assistant device name,
                # make the tracker the device's primary entity instead of
                # repeating the same name as both device and entity name.
                if body["device"].get("name") == body.get("name"):
                    body["name"] = None
            ok &= self._publish(
                f"homeassistant/device_tracker/{device_slug}/config", json.dumps(body)
            )

        state = {
            "in_zones": [f"zone.{self._settings.location}"] if seen else [],
            "state": self._settings.location if seen else self._settings.away,
        }
        ok &= self._publish(
            f"homeassistant/device_tracker/{device_slug}/state",
            json.dumps(state),
            retain=self._settings.mqtt_retain_state,
        )
        return ok

    def _register_signal_sensors(self, device: str) -> bool:
        """Register current and average RSSI sensors through MQTT Discovery."""
        if self._settings.signal_poll_interval <= 0:
            return True

        device_slug = self._device_slug(device)
        if device_slug in self._registered_signal_clients:
            return True

        device_info = self._device_info(device)
        sensors = (
            ("signal_strength", "Signal strength"),
            ("average_signal_strength", "Average signal strength"),
        )
        ok = True
        for suffix, name in sensors:
            topic_base = f"homeassistant/sensor/{device_slug}_{suffix}"
            body = {
                "state_topic": f"{topic_base}/state",
                "availability_topic": f"{topic_base}/availability",
                "payload_available": "online",
                "payload_not_available": "offline",
                "name": name,
                "device_class": "signal_strength",
                "unit_of_measurement": "dBm",
                "state_class": "measurement",
                "device": device_info,
                "unique_id": f"{device_slug}_{suffix}",
            }
            ok &= self._publish(f"{topic_base}/config", json.dumps(body))

        if ok:
            self._registered_signal_clients.add(device_slug)
        return ok

    def _clear_signal_publish_cache(self) -> None:
        """Forget published RSSI values so the next poll republishes current state."""
        self._last_published_signal_values.clear()
        self._last_published_signal_availability.clear()

    def _set_signal_availability(
        self,
        device: str,
        available: bool,
        sensor_suffix: str | None = None,
    ) -> bool:
        """Publish RSSI availability only when it actually changes."""
        if self._settings.signal_poll_interval <= 0:
            return True
        if not self._should_handle_device(device):
            return True

        ok = self._register_signal_sensors(device)
        device_slug = self._device_slug(device)
        suffixes = (
            [sensor_suffix]
            if sensor_suffix
            else ["signal_strength", "average_signal_strength"]
        )
        payload = "online" if available else "offline"
        for suffix in suffixes:
            cache_key = (device_slug, suffix)
            if self._last_published_signal_availability.get(cache_key) == available:
                continue
            published = self._publish(
                f"homeassistant/sensor/{device_slug}_{suffix}/availability",
                payload,
                retain=self._settings.mqtt_retain_state,
            )
            ok &= published
            if published:
                self._last_published_signal_availability[cache_key] = available
        return ok

    def _publish_signal_sample(self, device: str, sample: SignalSample) -> bool:
        """Publish RSSI only when values or availability change."""
        if not self._should_handle_device(device):
            return True
        if not self._register_signal_sensors(device):
            return False

        device_slug = self._device_slug(device)
        ok = True
        values = {
            "signal_strength": sample.signal,
            "average_signal_strength": sample.signal_avg,
        }
        for suffix, value in values.items():
            if value is None:
                ok &= self._set_signal_availability(device, False, suffix)
                continue

            cache_key = (device_slug, suffix)
            state_ok = True
            if self._last_published_signal_values.get(cache_key) != value:
                topic_base = f"homeassistant/sensor/{device_slug}_{suffix}"
                state_ok = self._publish(
                    f"{topic_base}/state",
                    str(value),
                    retain=self._settings.mqtt_retain_state,
                )
                ok &= state_ok
                if state_ok:
                    self._last_published_signal_values[cache_key] = value

            if state_ok:
                ok &= self._set_signal_availability(device, True, suffix)
        return ok

    @staticmethod
    def _wireless_interface(interface: str) -> str:
        """Convert hostapd.foo ubus object name to wireless interface name foo."""
        prefix = "hostapd."
        return interface[len(prefix) :] if interface.startswith(prefix) else interface

    def _active_signal_interfaces(self) -> list[str]:
        """Return interfaces that currently contain at least one tracked client."""
        with self._clients_lock:
            return [
                interface
                for interface, clients in self._online_clients.items()
                if any(self._should_handle_device(client) for client in clients)
            ]

    @staticmethod
    def _parse_signal_value(text: str) -> int | None:
        """Parse the first numeric dBm value from an iw signal field."""
        try:
            return int(float(text.strip().split()[0]))
        except (IndexError, TypeError, ValueError):
            return None

    def _parse_iw_station_dump(
        self, interface: str, output: str
    ) -> dict[str, SignalSample]:
        """Parse all tracked stations from one iw station dump in a single pass."""
        samples: dict[str, SignalSample] = {}
        current_device: str | None = None
        current_signal: int | None = None
        current_signal_avg: int | None = None

        def save_current() -> None:
            if current_device is None or not self._should_handle_device(current_device):
                return
            samples[current_device] = SignalSample(
                interface, current_signal, current_signal_avg
            )

        for raw_line in output.splitlines():
            line = raw_line.strip()
            if line.startswith("Station "):
                save_current()
                parts = line.split()
                current_device = parts[1].lower() if len(parts) > 1 else None
                current_signal = None
                current_signal_avg = None
                continue

            if current_device is None:
                continue
            if line.startswith("signal avg:"):
                current_signal_avg = self._parse_signal_value(
                    line[len("signal avg:") :]
                )
            elif line.startswith("signal:"):
                current_signal = self._parse_signal_value(line[len("signal:") :])

        save_current()
        return samples

    def _get_signal_samples(
        self, interfaces: list[str]
    ) -> tuple[dict[str, SignalSample], bool]:
        """Read RSSI only from interfaces that currently contain tracked clients."""
        samples: dict[str, SignalSample] = {}
        complete = True

        for interface in interfaces:
            wireless_interface = self._wireless_interface(interface)
            try:
                process = subprocess.run(
                    ["iw", "dev", wireless_interface, "station", "dump"],
                    capture_output=True,
                    text=True,
                    check=False,
                )
            except OSError as ex:
                self._logger.log(
                    f"Error running iw for {wireless_interface}: {ex}"
                )
                complete = False
                continue

            if process.returncode != 0:
                self._logger.log(
                    f"Error reading iw station dump for {wireless_interface}: "
                    f"{process.stderr.strip()}"
                )
                complete = False
                continue

            interface_samples = self._parse_iw_station_dump(
                interface, process.stdout
            )
            for device, sample in interface_samples.items():
                previous = samples.get(device)
                if previous is None or sample.strength > previous.strength:
                    samples[device] = sample

        return samples, complete

    def _known_tracked_devices(self) -> set[str]:
        """Return all tracked devices known from config, events, or current state."""
        with self._clients_lock:
            online_devices = {
                client
                for clients in self._online_clients.values()
                for client in clients
                if self._should_handle_device(client)
            }
        devices = online_devices | set(self._settings.params.keys()) | self._known_clients
        return {device for device in devices if self._should_handle_device(device)}

    def _update_signal_sensors(self) -> None:
        """Poll active Wi-Fi interfaces with iw and push RSSI telemetry to MQTT."""
        if self._settings.signal_poll_interval <= 0:
            return

        active_interfaces = self._active_signal_interfaces()
        known_devices = self._known_tracked_devices()

        # No tracked clients are currently associated, so avoid all fast RSSI
        # polling. Assoc events wake the poller immediately when one comes back.
        if not active_interfaces:
            for device in known_devices:
                self._set_signal_availability(device, False)
            return

        samples, complete = self._get_signal_samples(active_interfaces)
        for device, sample in samples.items():
            self._publish_signal_sample(device, sample)

        missing_devices = known_devices - samples.keys()
        if complete:
            for device in missing_devices:
                self._set_signal_availability(device, False)
        elif missing_devices:
            self._logger.log(
                "Signal poll incomplete; keeping previous availability for missing devices",
                True,
            )

    def _signal_loop(self) -> None:
        """Background loop for configurable local RSSI polling."""
        interval = self._settings.signal_poll_interval
        if interval <= 0:
            return
        self._logger.log(f"Starting signal polling every {interval:g} seconds")

        while not self._signal_stop.is_set():
            self._update_signal_sensors()
            self._signal_wakeup.wait(timeout=interval)
            self._signal_wakeup.clear()

    def start_signal_poller(self) -> None:
        """Start signal polling thread when enabled."""
        if self._settings.signal_poll_interval <= 0:
            self._logger.log("Signal polling disabled", True)
            return
        self._signal_thread = Thread(
            target=self._signal_loop,
            name="signal-poller",
            daemon=True,
        )
        self._signal_thread.start()

    def stop_signal_poller(self) -> None:
        """Stop signal polling thread."""
        self._signal_stop.set()
        self._signal_wakeup.set()
        if self._signal_thread and self._signal_thread.is_alive():
            self._signal_thread.join(timeout=2)

    def set_device_away(self, interface: str, device: str) -> None:
        """Mark a client as away in HA."""
        if not self._should_handle_device(device):
            return
        self._known_clients.add(device)
        with self._clients_lock:
            self._online_clients[interface].discard(device)
            other_interface = next(
                (
                    intf
                    for intf in set(self._settings.interfaces) - {interface}
                    if device in self._online_clients[intf]
                ),
                None,
            )
        if other_interface is not None:
            # Device is still connected to another interface -> ignore.
            self._logger.log(
                f"Device {device} still connected to {other_interface}, ignoring away event.",
                True,
            )
            self._signal_wakeup.set()
            return
        self._queue.put(QueueItem(device, interface, QueueItem.Action.DELETE))
        self._set_signal_availability(device, False)
        self._logger.log(f"Device {device} on {interface} is now away")

    def set_device_home(self, interface: str, device: str) -> None:
        """Add client to the 'add' queue."""
        if not self._should_handle_device(device):
            return
        self._known_clients.add(device)
        self._queue.put(QueueItem(device, interface, QueueItem.Action.ADD))
        with self._clients_lock:
            self._online_clients[interface].add(device)
        # Wake the poller so RSSI arrives immediately instead of waiting for the
        # next regular signal_poll_interval tick.
        self._signal_wakeup.set()
        self._logger.log(
            f"Device {device} on {interface} is now at {self._settings.location}"
        )

    def _get_all_online_devices(self) -> list[tuple[str, str]]:
        """Call ubus and get all online devices."""
        devices = []
        for interface in self._settings.interfaces:
            process = subprocess.run(
                ["ubus", "call", interface, "get_clients"],
                capture_output=True,
                text=True,
                check=False,
            )
            if process.returncode != 0:
                self._logger.log(
                    f"Error running ubus for interface {interface}: {process.stderr}"
                )
                continue
            try:
                response: dict = json.loads(process.stdout)
            except json.JSONDecodeError as ex:
                self._logger.log(f"Invalid ubus response for {interface}: {ex}")
                continue
            devices.extend([(interface, key.lower()) for key in response.get("clients", {})])
        return devices

    def _should_handle_device(self, device: str) -> bool:
        """Return whether a device is allowed to be tracked."""
        device = device.lower()
        if not self._settings.allow_list:
            return True
        return device in self._settings.allow_list

    def start_watchers(self) -> None:
        """Start ubus watcher threads for every interface."""
        self._logger.log(
            f"Starting ubus watchers on interfaces {self._settings.interfaces}"
        )
        for interface in self._settings.interfaces:
            watcher = UbusWatcher(interface, self.set_device_home, self.set_device_away)
            watcher.start()
            self._watchers.append(watcher)

    def stop_watchers(self) -> None:
        """Signal all ubus watchers to stop."""
        for watcher in self._watchers:
            watcher.stop()

    @property
    def stopped(self):
        """Should this Thread be stopped?"""
        return self._killed

    def stop(self, _signum: int | None = None, _frame: int | None = None):
        """Stop this thread as soon as possible."""
        self._logger.log("Stopping...")
        self.stop_watchers()
        self.stop_signal_poller()
        self._killed = True
        self._queue.put(QueueItem("quit", "", QueueItem.Action.QUIT))
        self._mqtt.disconnect()
        self._mqtt.loop_stop()

    def _do_full_sync(self, away_only=False, republish_known=False):
        """Perform a full sync of current devices and optionally rebuild HA state."""
        self._registered_clients = set()
        seen_now = set(self._get_all_online_devices())
        is_first_sync = self._last_seen_clients is None
        away = (self._last_seen_clients or set()) - seen_now
        self._last_seen_clients = seen_now

        for _interface, client in seen_now:
            if self._should_handle_device(client):
                self._known_clients.add(client)

        for interface, client in seen_now:
            if not away_only:
                self.set_device_home(interface, client)

        away_macs: set[str] = set()
        for interface, client in away:
            away_macs.add(client)
            self.set_device_away(interface, client)

        if is_first_sync or republish_known:
            # Rebuild discovery/state for devices that are already away. This is
            # required after an HA restart because MQTT Discovery config messages
            # are not retained, and an already-away client generates no new
            # disassoc event to recreate its entity/state.
            seen_macs = {client for _interface, client in seen_now}
            known_devices = set(self._known_clients) | set(self._settings.params.keys())
            for device in known_devices:
                if (
                    device in seen_macs
                    or device in away_macs
                    or not self._should_handle_device(device)
                ):
                    continue
                self._logger.log(
                    f"Republishing away state for known device {device}", True
                )
                self._queue.put(QueueItem(device, "", QueueItem.Action.DELETE))

            # RSSI discovery/state is rebuilt by the signal poller, outside the
            # MQTT callback thread.
            self._signal_wakeup.set()

    def run(self) -> None:
        """Main loop for the presence detector."""
        self._do_full_sync()
        self.start_watchers()
        self.start_signal_poller()

        mq_is_offline = False
        queue_timeout = (
            self._settings.fallback_sync_interval
            if self._settings.fallback_sync_interval > 0
            else None
        )

        while not self._killed:
            try:
                item: QueueItem = self._queue.get(timeout=queue_timeout)
            except queue.Empty:
                self._do_full_sync()
                continue

            if item.action == QueueItem.Action.QUIT:
                self._queue.task_done()
                break

            if item.action == QueueItem.Action.SYNC:
                self._do_full_sync(republish_known=True)
                self._signal_wakeup.set()
                self._queue.task_done()
                continue

            if self._ha_seen(item.device, item.action == QueueItem.Action.ADD):
                if mq_is_offline:
                    mq_is_offline = False
                    self._do_full_sync()
                    self._signal_wakeup.set()
            else:
                self._logger.log("MQTT broker seems to be offline, sleeping...")
                self._queue.put(item)
                mq_is_offline = True
                time.sleep(5)

            self._queue.task_done()


class UbusWatcher(Thread):
    """Watches live ubus events and signals presence detector of leave/join events."""

    def __init__(
        self,
        interface: str,
        on_join: Callable[[str, str], None],
        on_leave: Callable[[str, str], None],
    ) -> None:
        super().__init__()
        self._on_join = on_join
        self._on_leave = on_leave
        self._interface = interface
        self._killed = False

    def stop(self):
        """Stop this watcher thread."""
        self._killed = True

    def run(self) -> None:
        """Main loop for the ubus event watcher thread."""
        while not self._killed:
            # pylint: disable=consider-using-with
            ubus = subprocess.Popen(
                ["ubus", "subscribe", self._interface],
                stdout=subprocess.PIPE,
                text=True,
            )
            time.sleep(1)
            return_code = ubus.poll()
            if return_code is not None or ubus.stdout is None:
                ubus.wait()
                continue

            while not self._killed:
                line = ubus.stdout.readline()
                event = {}
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    pass
                if "assoc" in event:
                    self._on_join(self._interface, event["assoc"]["address"].lower())
                elif "disassoc" in event:
                    self._on_leave(
                        self._interface, event["disassoc"]["address"].lower()
                    )
            ubus.terminate()
            ubus.wait()


def main():
    """Main entrypoint: parse arguments and start all threads."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--config",
        help="Filename of configuration file",
        default="/etc/config/presence-detector.settings.json",
    )
    args = parser.parse_args()

    detector = PresenceDetector(args.config)
    detector.start()
    signal.signal(signal.SIGTERM, detector.stop)
    signal.signal(signal.SIGINT, detector.stop)

    while not detector.stopped:
        time.sleep(1)


if __name__ == "__main__":
    main()
