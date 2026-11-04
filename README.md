# mqtt-bridge

An MQTT v5 bridge to move data between MQTT servers.

It doesn't have any magical rewrite features yet, as I mainly just
wanted properties to be preserved across mosquitto instances, but it
does the thing.

## Ordering

Each `conn` delivers the messages it receives in order, as MQTT
requires for messages on a topic.  `conn name unordered url` handles
each message on its own thread instead, which is faster but can
reorder them, including leaving a stale retained value at the
destination.
