# RSSI telemetry extension

This fork keeps `device_tracker` presence event-driven through OpenWrt `hostapd` ubus `assoc`/`disassoc` events and adds two Home Assistant MQTT Discovery sensors for every tracked Wi-Fi client:

- `Signal strength` — current RSSI from `iwinfo assoclist` field `signal`.
- `Average signal strength` — averaged RSSI from `iwinfo assoclist` field `signal_avg`.

Both values are read locally on OpenWrt and pushed to the same MQTT broker already used by the device tracker. Home Assistant does not poll OpenWrt.

## OpenWrt requirement

The `iwinfo` ubus object must be available. On OpenWrt 25.12 this is provided by `rpcd-mod-iwinfo`:

```sh
apk add rpcd-mod-iwinfo
```

## Configuration

Use `allow_list` to limit tracking to specific Wi-Fi clients:

```json
"allow_list": [
  "aa:bb:cc:dd:ee:ff",
  "11:22:33:44:55:66"
]
```

An empty list (`"allow_list": []`) tracks all Wi-Fi clients. When the list is not empty, only the listed MAC addresses get a `device_tracker` and RSSI sensors. MAC matching is case-insensitive.

Add the optional signal polling setting:

```json
"signal_poll_interval": 5
```

The value is the local OpenWrt polling interval in seconds. Default: `5`. Set it to `0` to disable RSSI sensors.

For every tracked client Home Assistant receives two MQTT Discovery entities whose names are:

- `Signal strength`
- `Average signal strength`

They use `device_class: signal_strength`, unit `dBm`, and `state_class: measurement`, and are attached to the same Home Assistant device as the MQTT `device_tracker` through the client's MAC address.

When a client is no longer present in `iwinfo assoclist` on any monitored radio, both signal sensors become `unavailable`. A failed or malformed `iwinfo` response does **not** mark missing clients unavailable; the previous availability is kept until a complete poll succeeds.

If a MAC is briefly reported on more than one monitored Wi-Fi interface during roaming, the sample from the interface with the strongest `signal_avg` is used (falling back to `signal` if average RSSI is unavailable).

An `assoc` event wakes the signal poller immediately, so RSSI normally appears without waiting for the next regular interval.

## OpenWrt data source

For an interface such as `hostapd.phy0-ap0`, the signal poller calls:

```sh
ubus call iwinfo assoclist '{"device":"phy0-ap0"}'
```

and consumes the standard `results[].signal` and `results[].signal_avg` fields.
