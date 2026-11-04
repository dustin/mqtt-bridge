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

## Persistent sessions

With `session-expiry-interval` above 0, the broker keeps a conn's
subscriptions between runs.  MQTT gives a client no way to list them,
so a filter removed from the config keeps delivering messages, which
the bridge drops.  Run once with `session-expiry-interval=0` to start
that conn's session over.

## Delivery

Messages are forwarded at least once: the bridge subscribes at QoS 1,
so a message is acknowledged to its source only after it's been
forwarded, and one caught by a restart is delivered again.  That means
QoS 2 messages are forwarded at QoS 1, and a destination can see the
occasional duplicate.
