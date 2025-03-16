# Brainstorming

Author: Nicola Ricciardi


## Requirements

*py-busline* is an agnostic distributed-ready eventbus. This could allow us to share messages using different technologies, e.g. in-memory, MQTT and so on.


## Base concepts

*Starting point*: Orbitalis is **distributed-ready, plugin-oriented, micro-kernel architecture**.

Obviously, there are **core**s and **plugin**s which share information together using an eventbus (*py-busline*).

Different to canonical micro-kernel architecture in which there is only one core, in Orbitalis could be present **more than one cores** (and, obviously, more than one plugins).

Cores and plugins have a **standalone lifecycle**, therefore it is not needed to have *always* active cores and plugins.

To summarize, **Orbitalis is a *framework* in which more cores and plugins live together cooperating to achieve one or more tasks**.

We can friendly refer to cores and plugins using *orb*.

> [!IMPORTANT]
> In this version we suppose a secure environment without "evil orbs".  


## Overview

Given that orbs can be added to system in any moment, we must ensure a *resilient connection* system between them.

Orbitalis is **plugin-oriented**, therefore cores must request to plugins to connect to themselves.

Plugins can serve more than one cores based on their configuration.

Cores can *not* start without required plugins.

![Environment](doc/assets/images/environment.png)

### Initial handshaking

Initial handshaking between core and plugin is **DHCP-like**, i.e. it follows exactly DHCP phases.

![Handshake](doc/assets/images/core-plugin-handshake.png)

Obviously, given that more cores could send discover message at the same time, we must handle concurrency (e.g. reserving a slot, timeout, ...) 


### Connection management

#### Heartbeat

In order to ensure that a plugin serves its cores, it can send periodically an **heartbeat** to cores or it must provide a "route" to obtain information about its state.


#### Connection Termination

**Graceful** and **Non-graceful**.

![Connection termination](doc/assets/images/tcp_closes.jpg)



## Configurations

Both cores and plugins have a configuration.

**Core configuration** specifies:

- Which plugins are required in term of *IDs*, number for each type
- Optional plugins
- Blacklist and whitelist of plugins
- Plugin preferences

**Plugin configuration** specifies:

- How many cores can serve
- Whitelist and blacklist of cores
- Core preferences


## Security problems

Currently there are no security checks (e.g. no login) and we trust ID shared by orbs.


## Evolution

*Is this strict separation between cores and plugins concepts really useful?*

We could uniform cores and plugins in entities which are able to provide a functionality requiring a set of other entities to work; obviously this is not a micro-kernel architecture, but now there is not a structured system (i.e. having ACK) to share information between cores and between plugins. 

![Evolution](doc/assets/images/orbitalis-evolution.png)


***What's new?***

- **Dynamic dependencies resolution**
- **Orbs interaction tracing** for debug, logging events

***Issues***

- EventBus is a one-point-of-failure
- Overhead in messages handling, given that all orbs share the bus
- Security, no authorization system 



























