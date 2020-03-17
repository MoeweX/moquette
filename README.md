# Broadcast Group Broker Implementation
## based on Moquette

This project is part of the Broadcast Group Project which includes the following subprojects:
* [Broker Implementation](https://github.com/MoeweX/moquette): Extension of moquette that supports broadcast groups
* [Broadcast Group Simulation](https://github.com/MoeweX/broadcast-group-simulation): A simulation of the broadcast group formation process

Today, communication between IoT devices heavily relies on fog-based publish/subscribe (pub/sub) systems. Communicating via the cloud, however, results in a latency that is too high for many IoT applications. This project is about a fog-based pub/sub system that integrates edge resources to improve communication latency between end devices in proximity. To this end, geo-distributed broker instances organize themselves in dynamically sized broadcast groups that are connected via a scale-able fog broker.

If you use this software in a publication, please cite it as:

### Text
Jonathan Hasenburg, Florian Stanek, Florian Tschorsch, David Bermbach. **Managing Latency and Excess Data Dissemination in Fog-Based Publish/Subscribe Systems**. In: Proceedings of the Second IEEE International Conference on Fog Computing 2020 (ICFC 2020). IEEE 2020.

Experiments related with this publication are based on [commit](https://github.com/MoeweX/moquette/tree/a964cadc1083fe695eb5eb3022a8bef5ec9a7f06).

### BibTeX
```
@inproceedings{paper_hasenburg_broadcast_groups,
	title = {Managing Latency and Excess Data Dissemination in Fog-Based Publish/Subscribe Systems},
	booktitle = {Proceedings of the Second {IEEE} {International} {Conference} on {Fog} {Computing} (ICFC 2020)},
	author = {Hasenburg, Jonathan and Stanek, Florian and Tschorsch, Florian and Bermbach, David},
	year = {2020},
	publisher = {IEEE}
}
```

A full list of our [publications](https://www.mcc.tu-berlin.de/menue/forschung/publikationen/parameter/en/) and [prototypes](https://www.mcc.tu-berlin.de/menue/forschung/prototypes/parameter/en/) is available on our group website.

## What is Moquette?

* [Documentation reference guide](http://andsel.github.io/moquette/) Guide on how to use and configure Moquette
* [Google Group](https://groups.google.com/forum/#!forum/moquette-mqtt) Google Group to participate in development discussions.

Moquette aims to be a MQTT compliant broker. The broker supports QoS 0, QoS 1 and QoS 2.
Its designed to be evented, uses Netty for the protocol encoding and decoding part.

This fork contains the changes necessary to faciliate broadcast groups.
The main idea behind broadcast groups is to separate a set of Edge brokers from the rest of the network so that they can communicate directly using flooding, which is similar to creating wilcard bridges between brokers.
Global communication is handled via a (massively scaleable) Cloud broker.



