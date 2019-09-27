## What is Moquette?

* [Documentation reference guide](http://andsel.github.io/moquette/) Guide on how to use and configure Moquette
* [Google Group](https://groups.google.com/forum/#!forum/moquette-mqtt) Google Group to participate in development discussions.

Moquette aims to be a MQTT compliant broker. The broker supports QoS 0, QoS 1 and QoS 2.

Its designed to be evented, uses Netty for the protocol encoding and decoding part.

## What is this Fork?

This fork contains the changes necessary to faciliate broadcast groups.
The main idea behind broadcast groups is to separate a set of Edge brokers from the rest of the network so that they can communicate directly using flooding, which is similar to creating wilcard bridges between brokers.
Global communication is handled via a (massively scaleable) Cloud broker.

