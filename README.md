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

## Stats

Set `stat-prefix` on a connection to publish the bridge's counters to that
broker every 15 seconds as retained QoS 0 messages:

```
conn local mqtt://localhost/#bridge
     stat-prefix="mqtt-bridge"
```

This example uses `<prefix>` = `mqtt-bridge` and publishes:

- `<prefix>/<connection>/messages/received` — messages received from that connection.
- `<prefix>/<connection>/messages/sent` — messages forwarded to that connection.
- `<prefix>/<connection>/messages/failed` — messages that couldn't be delivered to that connection.

Stats aren't published on a connection without `stat-prefix`.  Multiple
connections can have a prefix, each with its own value.  The same counters
remain available through EKG (on `localhost:8000` by default).
