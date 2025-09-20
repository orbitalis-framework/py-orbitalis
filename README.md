# Orbitalis

**Orbitalis** is a **distributed**, **event-based micro-kernel** library for Python designed to simplify the construction of *loosely coupled, scalable, and modular systems*.

At its core, Orbitalis provides a lightweight yet powerful foundation for building event-driven applications using an event bus mechanism. It integrates natively with [Busline](https://github.com/orbitalis-framework/py-busline), a flexible and efficient event bus library that handles message distribution across system components, whether local or distributed.

If you don't want to read the user guide, you can skip to [Practical Example](#practical-example).

```mermaid
flowchart LR
      C1 --- EB1
      C1 --- EB2

      EB1 --- P3
      EB1 --- P1
      
      EB2 --- P2
      EB2 --- P4

      C1@{ shape: circle, label: "Core" }
      P1@{ shape: stadium, label: "Plugin" }
      P2@{ shape: stadium, label: "Plugin" }
      P3@{ shape: stadium, label: "Plugin" }
      P4@{ shape: stadium, label: "Plugin" }

      EB1@{ shape: das, label: "MQTT Broker" }
      EB2@{ shape: das, label: "Local EventBus" }
```

## User Guide

In this section we will explain how to use Orbitalis. If you want to know more details about this library, please read the [Advance Guide](#advance-guide). 

Every time-based values are expressed in seconds, because `asyncio.sleep` is used.

### Installation

```
pip install orbitalis
```

### Preliminary: Busline

If you know what [Busline](https://github.com/orbitalis-framework/py-busline) is, you can skip this section.

[Busline](https://github.com/orbitalis-framework/py-busline) is an agnostic asynchronous pub/sub library, which represents a good starting point to implement a wide set of application thanks to its pub/sub abstract layer.

It natively supports MQTT and Local event bus and you should know how to create pub/sub clients in order to allow your components to share messages.

We advice to read [Busline](https://github.com/orbitalis-framework/py-busline) documentation before continue.

### Overview

Orbitalis follows the **micro-kernel** (a.k.a. [**Plugin**](#plugin)) architectural pattern, where a minimal [**Core**](#core) (the kernel) handles only the essential system functions—like loading and managing modules, and routing events—while all additional features are implemented as independent [**Plugins**](#plugin) that interact via the event bus.

This design brings several advantages:

- **Separation of Concerns**: Each module is self-contained and focused on a specific task or domain
- **Extensibility**: New features can be added without modifying the core, reducing the risk of breaking existing functionality
- **Flexibility**: Modules can be enabled, disabled, or replaced at runtime
- **Maintainability**: Smaller code units are easier to test, debug, and reuse
- **Scalability**: Modules can be distributed across processes or machines and communicate via events


```mermaid
flowchart LR
      C1 --> P1
      C1 <--> P3

      C2 <--> P2
      P3 <--> C2
      P4 --> C2


      C1@{ shape: circle, label: "Core 1" }
      C2@{ shape: circle, label: "Core 2" }
      P1@{ shape: stadium, label: "Plugin 1" }
      P2@{ shape: stadium, label: "Plugin 2" }
      P3@{ shape: stadium, label: "Plugin 3" }
      P4@{ shape: stadium, label: "Plugin 4" }
```

Orbitalis allows to have more Cores and more Plugins, even in *different domains* thanks to [Busline](https://github.com/orbitalis-framework/py-busline) capabilities, which can be instantiate **at runtime** thanks to a powerful [handshake mechanism](#handshake). In fact, for example, we can have some plugins connected with MQTT and other with a Local event bus at the same time.
This allows you a powerful management of your components.

In other words, Orbitalis allows you to start cores and plugins, connect them together and execute plugin operations.
Cores and plugins can be started in any time and connections are created based on pre-defined policies.

Messages used by Orbitalis are **Avro messages** because we need input and output schemas.

```mermaid
flowchart LR
      C1 <--> EB1
      C1 <--> EB2

      C2 <--> EB1
      C2 <--> EB2
    
      EB1 <--> P3
      EB1 <--> P1
      
      EB2 <--> P2
      EB2 <--> P4

      C1@{ shape: circle, label: "Core 1" }
      C2@{ shape: circle, label: "Core 2" }
      P1@{ shape: stadium, label: "Plugin 1" }
      P2@{ shape: stadium, label: "Plugin 2" }
      P3@{ shape: stadium, label: "Plugin 3" }
      P4@{ shape: stadium, label: "Plugin 4" }

      EB1@{ shape: das, label: "MQTT Broker" }
      EB2@{ shape: das, label: "Local EventBus" }
```


### Orbiter

`Orbiter` is the base class which provides common capabilities to components. 
Therefore, you can use the provided methods both in [Cores](#core) and [Plugins](#plugin).

It manages _[pending requests](#pending-requests), [connections](#connections), [keepalive](#keepalive) and [connection close procedure](#connection-close-procedure)_. In addiction, it has useful _shared methods_ and main _loop_.

```mermaid
classDiagram
    Orbiter <|-- Core
    Orbiter <|-- Plugin
```

Main public attributes:

- `identifier` is the _unique_ identifier
- `eventbus_client` is a [Busline](https://github.com/orbitalis-framework/py-busline) client, used to send events
- `discover_topic` specifies topic used to send discover messages
- `raise_exceptions` if `True`, exceptions are raised, otherwise they are managed by try/catch
- `loop_interval` specifies how often the loop iterations are called (it is a minimum value, because maximum depends on weight of operations in loop)
- `with_loop` set to `False` if you don't want the loop (care about _what_ [loop](#loop) do)
- `close_connection_if_unused_after` if not None, it specifies how many seconds can pass without use a connection, then it is closed
- `pending_requests_expire_after` if not None, it specifies how many seconds can pass before that a pending request is discarded 
- `consider_others_dead_after` states how many seconds can pass before that a remote orbiter is considered dead if no keepalive arrives
- `send_keepalive_before_timelimit` states how many seconds before a keepalive message is sent that other remote orbiter considers current orbiter dead
- `graceful_close_timeout`: states how many seconds a graceful close connection can be pending

Main hooks:

- `_get_on_close_data`: used to obtain data to send on close connection, by default None is returned
- `_on_starting`: called before starting
- `_internal_start`: actual implementation to start the orbiter
- `_on_started`: called after starting
- `_on_stopping`: called before stopping
- `_internal_stop`: actual implementation to stop the orbiter
- `_on_stopped`: called after stopping
- `_on_promote_pending_request_to_connection`: called before promotion
- `_on_keepalive_request`: called on keepalive request, before response
- `_on_keepalive`: called on inbound keepalive
- `_on_graceless_close_connection`: called before graceless close connection request is sent
- `_on_close_connection`: called when a connection is closed
- `_on_graceful_close_connection`: called before sending graceful close connection request
- `_on_loop_start`: called on loop start
- `_on_new_loop_iteration`: called before every loop iteration
- `_on_loop_iteration_end`: called at the end of every loop iteration
- `_on_loop_iteration`: called during every loop iteration

Main methods:

- `retrieve_connections`: retrieve all connections which satisfy query
- `discard_expired_pending_requests`: remove expired pending requests and return total amount of discarded requests 
- `close_unused_connections`: send a graceful close request to all remote orbiter if connection was unused based on `close_connection_if_unused_after`
- `force_close_connection_for_out_to_timeout_pending_graceful_close_connection`: send graceless close connection based on `graceful_timeout` if a connection is in close pending due to graceful close connection 
- `update_acquaintances`: update knowledge about keepalive request topics, keepalive topics and dead time
- `have_seen`: update last seen for remote orbiter
- `send_keepalive`
- `send_keepalive_request`
- `send_all_keepalive_based_on_connections`: send keepalive messages to all remote orbiters which have a connection with this orbiter
- `send_keepalive_based_on_connections_and_threshold`: send keepalive messages to all remote orbiters which have a connection with this orbiter only if `send_keepalive_before_timelimit` seconds away from being considered dead this orbiter
- `send_graceless_close_connection`: send a graceless close connection request to specified remote orbiter, therefore, self side connection will be closed immediately
- `send_graceful_close_connection`: send a graceful close connection request to specified remote orbiter, therefore self side connection is not close immediately, but ACK is waited
- `_close_self_side_connection`: close local connection with remote orbiter, therefore only this orbiter will no longer be able to use connection. Generally, a close connection request was sent before this method call.


#### Loop

Every orbiter has an internal **loop** which performs periodically operations. Automatic loop initialization can be avoided setting `with_loop=False`.

Loop is stopped when `stop` method is called, but you can stop it prematurely using `stop_loop` method. 
If you want to pause loop, you can use `pause_loop` and `resume_loop`.

Additionally to hooks, the following operations (*already discussed above*) are executed in parallel:

- `_on_loop_iteration`
- `close_unused_connections`
- `discard_expired_pending_requests`: 
- `force_close_connection_for_out_to_timeout_pending_graceful_close_connection`
- `send_keepalive_based_on_connections_and_threshold`

Hooks:

- `_on_loop_start`: called on loop start
- `_on_new_loop_iteration`: called before every loop iteration
- `_on_loop_iteration_end`: called at the end of every loop iteration
- `_on_loop_iteration`: called during every loop iteration


### Introduction to Core-Plugin Communication

Orbitalis implemented different communication protocol which are used to ensure that orbiters can share information and connect them together.

If you only use this library, you should not care about their implementation details, because every was made by us. Instead, if you want to know more or if you want to contribute, please read the [Advance Guide](#advance-guide). 

#### Handshake

To allow cores and plugins to create connections a handshake mechanism was implemented based on how DHCP works.

![DHCP](doc/assets/images/dhcp.jpg)

There are 4 steps:

1. **Discover**: message used by cores to notify plugins of their presence and to ask operations connections, core provides a full set of pluggable operations with related information
2. **Offer**: Message used by plugins to response to discover message, providing their base information and a list of offered operations. List of offered operations can be smaller than fullset provided by discover
3. **Reply**
   - **Request**: message used by core to formally request an operation. Every operation has own request. Core provides additional information to finalize the connection
   - **Reject**: message used by core to formally reject an operation (e.g., not needed anymore). Every operation has own reject
4. **Response**
   - **Confirm**: message used by plugins to confirm the connection creation
   - **OperationNoLongerAvailable**: message used by plugins to notify core that operation is no longer available


When an [Pending Request](#pending-requests) is *confirmed*, then a [Connection](#connections) is generated (and the related request is removed).

```mermaid
sequenceDiagram
    Core->>Plugin: Discover
    Plugin->>Core: Offer
    par Reply for each offered operation
        alt is still needed
            Core->>Plugin: Request
        else not needed anymore
            Core->>Plugin: Reject
        end
    end
    par Response for each requested operation
        alt is still available
            Plugin->>Core: Confirm
        else operation no longer available
            Plugin->>Core: OperationNoLongerAvailable
        end
    end
```

Orbiters which are related to the same context must use the same **discover topic** (by default `$handshake.discover`).
It can be set using `discover_topic` attribute.

Other topics are automatically generated, but generation can be modified overriding `_build_*` methods.

We must notice that discover and offer messages are also used to share information about presence of cores/plugins.
Theoretically, this means that a plugin may send an offer without an explicit discover, 
for example if a connection is closed and a slot for an operation becomes available. Anyway, this is not performed in current implementation.


#### Connections

`Connection` is a link between a core and a plugin **related to a *single* operation**, therefore more connections can be present between same core and plugin.

Regularly, connections are created after a handshake procedure, promoting a [Pending Request](#pending-requests). Generally you don't need to know how to create a connection, but if you want, you can read the [Advance Guide](#advance-guide).

```mermaid
flowchart TD
    P["Pending Request"] --> C["Connection"]
```

`Connection` class store all information about a connection:

- `operation_name`
- `remote_identifier`
- `incoming_close_connection_topic`: topic on which close connection request arrives from remote orbiter
- `close_connection_to_remote_topic`: topic which orbiter must use to close connection with remote orbiter
- `lock`: `asyncio.Lock`, used to synchronize connection uses
- `input`: acceptable `Input`
- `output`: sendable `Output`
- `input_topic`
- `output_topic`
- `soft_closed_at`: used during graceful close connection
- `created_at`: connection creation datetime
- `last_use`: datetime of last use

`last_use` must be updated **manually**, if you want to update it, using `touch` method on each connection (remember to *lock* the connection).

For example, when a new event arrives:

```python
# Following a custom plugin (MyPlugin) with an operation "my_operation"
@dataclass
class MyPlugin(Plugin):
    @operation(
        # operation name
        name="my_operation",
        
        # operation is fed with Int64Message messages (integer)
        input=Input.from_message(Int64Message),
        
        # operation doesn't send any output
        output=Output.no_output()
    )
    async def its_event_handler(self, topic: str, event: Event[...]):

        # Retrieve connections related to the input topic and the operation name
        connections = self.retrieve_connections(
            input_topic=topic, 
            operation_name="my_operation"
        )
        
        # Touch each operation to update `last_use`
        for connection in connections:
            async with connection.lock:     # lock connection to be async-safe
                connection.touch()
```

Fortunately, there is an useful method called `_retrieve_and_touch_connections` which encapsulates exactly that code:

```python
# Same plugin of previous example, 
# but in which _retrieve_and_touch_connections is used

@dataclass
class MyPlugin(Plugin):
    @operation(
        name="my_operation",    # operation's name, i.e. "my_operation"
        input=Input.from_message(Int64Message), # operation's input, i.e. an integer number 
        output=Output.no_output()   # operation's output, i.e. no output are produced
    )
    async def its_event_handler(self, topic: str, event: Event[...]):

        # Retrieves connections related to this operation and touches them
        connections = await self._retrieve_and_touch_connections(
            input_topic=topic, 
            operation_name="my_operation"
        )

        # ...operation's logic
```

Orbiter connections are stored in `_connections` attribute.

You can manage them using following methods:

- `_add_connection`
- `_remove_connection` (_does not lock automatically the connection_)
- `_connections_by_remote_identifier`: retrieves connection based on remote identifier
- `retrieve_connections`: to query connections
- `_find_connection_or_fail`: find _the_ connection based on `operation_name` and `input_topic` 
- `close_unused_connections` based on `close_connection_if_unused_after` and `last_use`


> [!IMPORTANT]
> Remember that potentially you can have more connections associated with the same pair input topic and operation name, based on how you have defined builder methods for input/output topics. This is the reason behind the fact that you don't receive a single connection by default as an input parameter. Anyway, if you are sure that **only one connection** is present, you can use `_find_connection_or_fail` as specified above.

#### Connection Close Procedure

An [orbiter](#orbiter) (Core or Plugin) can close a connection in every moment. There are two ways to close a connection: *Graceless* or *Graceful*.

In the following example we suppose an orbiter `"my_orbiter1"` that closes connection with another orbiter `"my_orbiter2"` related to operation `"my_operation"`.

In **Graceless** procedure the orbiter sends a `GracelessCloneConnectionMessage` to remote one and close connection immediately:

```mermaid
sequenceDiagram
    Orbiter 1->>Orbiter 1: Close connection
    Orbiter 1->>Orbiter 2: Graceless close connection
    Orbiter 2->>Orbiter 2: Close connection
```

```python
# `orbiter1` close connection immediately 
# with "my_orbiter2" related to operation "my_operation"
orbiter1.send_graceless_close_connection(
    remote_identifier="my_orbiter2",
    operation_name="my_operation"
)
```

In **Graceful** the orbiter sends a `GracefulCloseConnectionMessage` to remote one. Remote orbiter is able to perform some operations before connection is actually closed. Remote orbiter must send a `CloseConnectionAckMessage`, after that connection is closed. If `graceful_close_timeout` is not `None` is used to send graceless close connection if ACK is not sent. 
You can force timeout check using `force_close_connection_for_out_to_timeout_pending_graceful_close_connection` method.

```mermaid
sequenceDiagram
    Orbiter 1->>Orbiter 2: Graceful close connection
    activate Orbiter 1
    Orbiter 2->>Orbiter 2: Some operations
    Orbiter 2->>Orbiter 2: Close connection
    Orbiter 2->>Orbiter 1: Close connection ACK
    deactivate Orbiter 1
    Orbiter 1->>Orbiter 1: Close connection
```

```python
# `orbiter1` close connection gracefully 
# with "my_orbiter2" related to operation "my_operation"
orbiter1.send_graceful_close_connection(
    remote_identifier="my_orbiter2",
    operation_name="my_operation"
)
```

When all connections with a remote orbiter are closed, orbiter unsubscribes itself from topics in `_unsubscribe_on_full_close_bucket` field.

Both during graceful or graceless method call, you can provide data (`bytes`) which will be sent when connection is actually closed.
For example, considering graceful procedure:

```python
orbiter1.send_graceful_close_connection(
    remote_identifier="my_orbiter2",    # identifier of orbiter related to connection to close
    operation_name="my_operation",
    data=bytes(...)     # serialize your data
)
```

Hooks:

- `_on_graceless_close_connection`: called before graceless close connection request is sent
- `_on_close_connection`: called when a connection is closed
- `_on_graceful_close_connection`: called before sending graceful close connection request

> [!NOTE]
> These methods are also called when a connection is closed using method `close_unused_connections` (used to close unused connections based on `close_connection_if_unused_after` and `last_use`).

#### Keepalive

**Keepalive mechanism** allows orbiters to preserve connections during the time. Every orbiter must send a keepalive to all own linked orbiters.

An orbiter can _request_ a keepalive using `send_keepalive_request` method, which sends a `KeepaliveRequestMessage`.

```mermaid
sequenceDiagram
    Orbiter 1->>Orbiter 2: Keepalive request
    Orbiter 2->>Orbiter 1: Keepalive
```

Keepalive are sent using `KeepaliveMessage` messages. You can manually send a keepalive using `send_keepalive` method.

In addiction, `send_all_keepalive_based_on_connections` and `send_keepalive_based_on_connections_and_threshold` are provided.
`send_all_keepalive_based_on_connections` sends a keepalive to all remote orbiters which have an opened connection, 
instead `send_keepalive_based_on_connections_and_threshold` sends a keepalive to all remote orbiters which have an opened connection 
only if it is in range `send_keepalive_before_timelimit` seconds before remote orbiter considers it _dead_.

You can know which are dead remote orbiters thanks to `dead_remote_identifiers` property.

Main related fields:

- `_others_considers_me_dead_after`: dictionary which contains `remote_identifier => time`, used to know when a keepalive message must be sent
- `_remote_keepalive_request_topics`: dictionary which contains `remote_identifier => keepalive_request_topic`, used to send keepalive requests to remote orbiters
- `_remote_keepalive_topics`: dictionary which contains `remote_identifier => keepalive_topic`, used to send keepalive to remote orbiters
- `_last_seen`: dictionary which contains `remote_identifier => last_seen`, used to know if a remote orbiter must be considered *dead*
- `_last_keepalive_sent`: dictionary which contains `remote_identifier => last_keepalive_sent`, used to know if a keepalive message must be sent

Hooks:

- `_on_keepalive_request`: called on keepalive request, before response
- `_on_keepalive`: called on inbound keepalive



### Plugin

`Plugin` is an [Orbiter](#orbiter) and it is basically an _operations provider_. In a certain sense, plugins lent possibility to execute their operations.

In particular, every plugin has a set of operations which are exposed to other components (i.e., cores).
Only connected [Cores](#core) should execute operations, but this is not strictly ensured, you should check if there is a valid connection during operation elaboration.
You can check this using `retrieve_connections` or `_find_connection_or_fail`.

A plugin is a state machine which follows this states:

```mermaid
stateDiagram-v2
    [*] --> CREATED
    CREATED --> RUNNING: start()
    RUNNING --> STOPPED: stop()
    STOPPED --> RUNNING: start()
    STOPPED --> [*]
```

You can easily create a new plugin inheriting `Plugin` abstract class. Remember to use `@dataclass` For example:

```python
@dataclass
class MyPlugin(Plugin):
    ... # your plugin's operations and logic
```


Main hooks:

- `_on_new_discover`: called when a new discover message arrives
- `_on_reject`: called when a reject message arrives
- `_setup_operation`: called to set up operation when connection is created
- `_on_request`: called when a new request message arrives
- `_on_reply`: called when a new reply message arrives

Main methods:

- `send_offer`: send a new offer message in given topic to given core identifier (it should be used only if you want to send an offer message manually)
- `with_operation`: generally used during creation, allows you to specify additional operations (but generally we use decorator)
- `with_custom_policy`: generally used during creation, allows you to specify a custom operation policy


#### Operations

An **operation** (`Operation`) represents a feature of a plugin, which is exposed and can be executed remotely. 
Operations are managed by `OperationsProviderMixin` which also provides builder-like methods.

Every operation has the following attributes:

- `name`: unique name which identify the operation
- `handler`: [Busline](https://github.com/orbitalis-framework/py-busline) handler, which will be used to handle inbound events
- `policy`: specifies default operation lending rules, you can override this using `with_custom_policy` method or modifying `operations` attribute directly
- `input`: specifies which is the acceptable input
- `output`: specifies which is the sendable output

Even if output can be specified, if a `Core` doesn't need it, it should not sent. Obviously, you decide which messages must be sent in which topics, so you must ensure the compliance.

You can **add or modify manually operations** to a plugin thanks to `operations` attribute, otherwise you can use `@operation` **decorator**.

> [!IMPORTANT]
> Obviously, if the operation's input is `Input.no_input()` you **must** add manually the operation, because there isn't an event handler. You can see an example of it in [periodic operation](#periodic-operations) section.

`@operation` if you don't provide an `input` or an `output`, they are considered "no input/output" (see [input/output](#input--output)). 
If you don't specify `default_policy`, `Policy.no_constraints()` is assigned.

`@operation` automatically add to `operations` attributes the generated `Operation` object, related to (operation) `name` key.

For example, if you want to create a plugin having an operation `"lowercase"` which supports strings as input and produces strings (without lent constraints, i.e. no policy):

```python
@dataclass
class LowercaseTextProcessorPlugin(Plugin):

    @operation(
        name="lowercase",       # operation's name
        input=Input.from_message(StringMessage),    # operation's input
        output=Output.from_message(StringMessage)   # operation's output
        # no policy is specified => Policy.no_constraints()
    )
    async def lowercase_event_handler(self, topic: str, event: Event[StringMessage]):
        # NOTE: input message specified in @operation should be the same of 
        # what is specified as type hint of event parameter

        # Retrieve input string value, remember that it is wrapped into StringMessage
        input_str = event.payload.value

        lowercase_text = input_str.lower()  # process the string

        # Retrieve and touch related connections
        connections = await self._retrieve_and_touch_connections(
            input_topic=topic, 
            operation_name="lowercase"
        )

        tasks = []
        for connection in connections:

            # Only if the connection expects an output
            # it is published in the related topic
            # specified by `connection.output_topic`
            if connection.has_output:
                tasks.append(
                    asyncio.create_task(
                        self.eventbus_client.publish(
                            connection.output_topic,
                            lowercase_text  # will be wrapped into StringMessage
                        )
                    )
                )

        await asyncio.gather(*tasks)    # wait publishes
```

> [!NOTE]
> Method name is not related to operation's name.


##### Periodic operations

If you want to elaborate something periodically, you can use provided orbiter's loop. This allows you to avoid [custom loop](#custom-loop), even if you can create them.

We advice to use an auxiliary field which contains last elaboration datetime.

For example, we suppose to have a [plugin](#plugin) which has an operation `randint` which periodically send a random number to connected cores.

```python
@dataclass
class RandomNumberPlugin(Plugin):
    """
    This plugin has only one operation which sends periodically a random number
    """

    last_sent: Optional[datetime] = field(default=None)     # will be used to check if a new value must be sent

    def __post_init__(self):
        super().__post_init__()

        # Manually define "randint" operation
        self.operations["randint"] = Operation(
            name="randint",     # operation's name
            input=Input.no_input(),     # no inputs are expected
            handler=None,               # handler is not needed, given that no inputs must be processed
            output=Output.from_message(Int64Message),   # integers will be sent to cores
            policy=Policy.no_constraints()          # no constraint during handshake
        )

    @override
    async def _on_loop_iteration(self):     # we use loop iteration hook to provide periodic operations

        now = datetime.now()

        # Only if the enough time is elapsed the operation will be executed
        if self.last_sent is None or (now - self.last_sent).seconds > 2:    # send a new random int every 2 seconds
            self.last_sent = now    # update timer for next iteration
            await self.__send_randint()


    async def __send_randint(self):
        random_number = random.randint(0, 100)      # generate a new random number, it will be sent

        connections = await self._retrieve_and_touch_connections(operation_name="randint")  # retrieve current core connections

        tasks = []
        for connection in connections:
            if connection.has_output:   # check if output is expected (it should be...)
                tasks.append(
                    asyncio.create_task(
                        self.eventbus_client.publish(   # send random int
                            connection.output_topic,
                            random_number       # message (output)
                        )
                    )
                )

        await asyncio.gather(*tasks)
```

> [!IMPORTANT]
> Remember to enable loop (*avoid* `with_loop=False`) during plugin instantiation.

##### Policy

`Policy` allows you to specify for an operation:

- `maximum` amount of connections
- `allowlist` of remote orbiters
- `blocklist` of remote orbiters

Obviously, you can specify `allowlist` or `blocklist`, not both.

If you don't want constraints: `Policy.no_constraints()`.

If you use `@operation`, you can specified a *default policy*, which is what is used if you don't override it during plugin initialization.
Anyway, you could manage manually operations' policies, but we advise against.

```python
@dataclass
class LowercaseTextProcessorPlugin(Plugin):

    @operation(
        name="lowercase",
        input=Input.from_message(StringMessage),
        output=Output.from_message(StringMessage),
        default_policy=Policy(
            ... # insert here you constraints (e.g. allowlist or maximum)
        )
    )
    async def lowercase_event_handler(self, topic: str, event: Event[StringMessage]):
        ...     # operation's logic
```

As already mentioned, if you want to override default policy for a plugin's operation you can use `with_custom_policy`.

For example, if you want to add an allowlist (within core identifier `"my_core"`) for the above plugin, related to operation `"lowercase"`:

```python
plugin = LowercaseTextProcessorPlugin(...)
        .with_custom_policy(        # default policy specified with @operation will be override
            operation_name="lowercase",     # target operation
            policy=Policy(allowlist=["my_core"])    # new custom policy
        )
```

##### Input & Output

`Input` and `Output` are both `SchemaSpec`, i.e. the way to specify a schema set.

In a `SchemaSpec` we can specify `support_empty_schema` if we want to support payload-empty events, 
`support_undefined_schema` if we want to accept every schema and/or a schema list (`schemas`). A schema is a string.

We do have an input or an output only if:

```python
support_undefined_schema or support_empty_schema or has_some_explicit_schemas
```

In other words, we must always specify an `Input`/`Output` even if it is "no input/output". 
We can easily generate a "no input/output" thanks to: 

```python
input = Input.no_input()    # create new "no input" 

# `has_input` property is used to check if input is expected
assert not input.has_input

output = Output.no_output()    # create new "no output" 

# `has_output` property is used to check if output is expected
assert not output.has_output
```

If we want to generate a filled `Input`/`Output`:

```python
# Input related with MyMessage
Input.from_message(MyMessage)

# Input related with MyMessage, same as above
Input.from_schema(MyMessage.avro_schema())

# Input which accepts empty events
Input.empty()

# Input which accepts any payload
Input.undefined()

# Input which accepts related primitive data 
Input.int32()
Input.int64()
Input.float32()
Input.float64()
Input.string()

# Manual initialization of an Input object
Input(schemas=[...], support_empty_schema=True, support_undefined_schema=False)
```

By default, given that we use Avro JSON schemas, two schemas are compatible if the dictionary version of both is equal or
if the string version of both is equal.

For this reason, you must avoid time-based default value in your classes, because Avro set as default a time-variant value. 
Therefore, in this way, two same-class schema are **different**, even if they are related to the same class.

```python
created_at: datetime = field(default_factory=lambda: datetime.now())    # AVOID !!!
```



#### Plugins inheritance example

In this example, we will show you how to implement a **hierarchy of plugins**.

We will define `LampPlugin` abstract class plugin which provides a common operation `"get_status"`, then it will be inherited by `LampXPlugin` and `LampYPlugin` which add more operations.

```python
class LampStatus(StrEnum):
    """
    Utility enum to define possible plugin statuses
    """

    ON = "on"
    OFF = "off"


@dataclass
class StatusMessage(AvroMessageMixin):
    """
    Custom message defined to share status of lamps
    """

    lamp_identifier: str
    status: str     # "on" or "off"
    created_at: datetime = None # it is an Avro message, avoid default of time-variant fields

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()    # default created_at value


@dataclass
class LampPlugin(Plugin, ABC):
    """
    Plugin to control a smart plugin which has an energy-meter
    """

    # Custom plugin attributes
    kw: float  # plugin energy consumption
    status: LampStatus = field(default=LampStatus.OFF)
    on_at: Optional[datetime] = field(default=None) # datetime of on request
    total_kwh: float = field(default=0.0)   # total consumption history

    @property
    def is_on(self) -> bool:
        return self.status == LampStatus.ON

    @property
    def is_off(self) -> bool:
        return self.status == LampStatus.OFF

    def turn_on(self):
        self.status = LampStatus.ON

        if self.on_at is None:
            self.on_at = datetime.now()

    def turn_off(self):
        """
        Turn off this plugin and update consumption history
        """

        self.status = LampStatus.OFF

        if self.on_at is not None:
            # Update total consumption:
            self.total_kwh += self.kw * (datetime.now() - self.on_at).total_seconds() / 3600

            self.on_at = None

    @operation(
        name="get_status",
        input=Input.empty(),
        output=Output.from_message(StatusMessage)
    )
    async def get_status_event_handler(self, topic: str, event: Event):
        connections = await self._retrieve_and_touch_connections(input_topic=topic, operation_name="get_status")

        # Only one connection should be present on inbound topic
        assert len(connections) == 1

        connection = connections[0]

        assert connection.output_topic is not None
        assert connection.output.has_output

        # Manually touch the connection
        async with connection.lock:
            connection.touch()

        # Send output to core
        await self.eventbus_client.publish(
            connection.output_topic,
            StatusMessage(self.identifier, str(self.status))
        )

    @abstractmethod
    async def turn_on_event_handler(self, topic: str, event: Event):
        raise NotImplemented()

    @abstractmethod
    async def turn_off_event_handler(self, topic: str, event: Event):
        raise NotImplemented()
```

```python
@dataclass
class LampXPlugin(LampPlugin):
    """
    Specific plugin related to brand X of smart lamps.
    This type of lamps doesn't have additional features
    """

    @operation(     # add new operation with name: "turn_on"
        name="turn_on",
        input=Input.empty()     # accepts empty events
    )
    async def turn_on_event_handler(self, topic: str, event: Event):
        self.turn_on()

    @operation(     # add new operation with name: "turn_off"
        name="turn_off",
        input=Input.empty()     # accepts empty events
    )
    async def turn_off_event_handler(self, topic: str, event: Event):
        self.turn_off()
```

```python
# Create new plugin X plugin
lamp_x_plugin = LampXPlugin(
    identifier="lamp_x_plugin",
    eventbus_client=...,    # provide Busline client
    raise_exceptions=True,
    with_loop=False,

    kw=24      # LampPlugin-specific attribute
).with_custom_policy(   # override custom policy related to operation "turn_on"
    operation_name="turn_on",
    policy=Policy(allowlist=["smart_home"])
)
```

```python
@dataclass(frozen=True)
class TurnOnLampYMessage(AvroMessageMixin):
    """
    Custom message to turn on plugin of brand Y.
    You can provide a "power" value which will be used to
    control brightness (and energy consumption)
    """

    power: float = field(default=1)

    def __post_init__(self):
        assert 0 < self.power <= 1

@dataclass(frozen=True)
class TurnOffLampYMessage(AvroMessageMixin):
    """
    Custom message to turn off plugin of brand Y.
    You can reset energy-meter setting True the flag
    """
    reset_consumption: bool = field(default=False)



@dataclass(kw_only=True)
class LampYPlugin(LampPlugin):
    """
    Specific plugin related to brand Y of smart lamps.
    These lamps are able to manage brightness level
    thanks to "power" attribute
    """

    power: float = field(default=1)

    @override
    def turn_off(self):
        """
        Overridden version to turn off the plugin and compute energy consumption 
        also based on power field
        """

        self.status = LampStatus.OFF

        if self.on_at is not None:
            self.total_kwh += self.power * self.kw * (datetime.now() - self.on_at).total_seconds() / 3600

            self.on_at = None

    @operation(     # add new operation with name: "turn_on"
        name="turn_on",
        input=Input.from_message(TurnOnLampYMessage)   # accepts TurnOnLampYMessage messages (checking its Avro schema)
    )
    async def turn_on_event_handler(self, topic: str, event: Event[TurnOnLampYMessage]):
        self.turn_on()
        self.power = event.payload.power

    @operation(     # add new operation with name: "turn_off"
        name="turn_off",
        input=Input.from_schema(TurnOffLampYMessage.avro_schema())   # accepts TurnOffLampYMessage messages
    )
    async def turn_off_event_handler(self, topic: str, event: Event[TurnOffLampYMessage]):
        self.turn_off()

        # Reset energy-meter based on operation input
        if event.payload.reset_consumption:
            self.total_kwh = 0
```

```python
# Create a new plugin Y plugin
lamp_y_plugin = LampYPlugin(
    identifier="lamp_y_plugin",
    eventbus_client=...,    # provide a Busline client
    raise_exceptions=True,
    with_loop=False,

    kw=42   # custom field
)
```



### Core

`Core` is an [Orbiter](#orbiter) and it is the component which connects itself to [plugins](#plugin), in order to be able to **execute their operations**.

We must notice that Orbitalis' Cores are able to manage operations having different inputs/outputs (but same name), thanks to (Avro) schemas.

We can use a core to execute operations and collect outputs (if they are present).

```mermaid
sequenceDiagram
    Core->>Plugin: Execute Operation (with Input)
    Plugin-->>Core: Output (gathered by sink)
```

A core can also receive messages without an explicit `execute` call, e.g. a plugin can send information periodically, such as a report.

```mermaid
sequenceDiagram
    Plugin->>Core: Message (gathered by sink)
    Plugin->>Core: Message (gathered by sink)
    Plugin->>Core: Message (gathered by sink)
```

We can specify needed operations which make a core compliant with respect to our needs. In fact, `Core` follows these states changes:

```mermaid
stateDiagram-v2
    [*] --> CREATED
    CREATED --> COMPLIANT: start()
    CREATED --> NOT_COMPLIANT: start()
    COMPLIANT --> NOT_COMPLIANT
    NOT_COMPLIANT --> COMPLIANT
    COMPLIANT --> STOPPED: stop()
    NOT_COMPLIANT --> STOPPED: stop()
    STOPPED --> [*]
```

`COMPLIANT` when all needs are satisfied, otherwise `NOT_COMPLIANT`.

Main public attributes:

- `discovering_interval`: interval between two discover messages (only when loop is enabled)
- `operation_requirements`: specifies which operations are needed to be compliant, specifying their constraints and optionally the default setup data or the sink
- `operation_sinks` (see [sinks](#sinks))

Main hooks:

- `_on_compliant`: called when core becomes compliant
- `_on_not_compliant`: called when core becomes not compliant
- `_on_send_discover`: called before discover message is sent
- `_get_setup_data`: called to obtain setup data which generally will be sent to plugins. By default, `default_setup_data` is used
- `_on_new_offer`: called when a new offer arrives
- `_on_confirm_connection`: called when a confirm connection arrives
- `_on_operation_no_longer_available`: called when operation no longer available message arrives
- `_on_response`: called when response message arrives

Main methods:

- `current_constraint_for_operation`: returns current constraint for operation based on current connections
- `is_compliant_for_operation`: evaluate at run-time if core is compliant for given operation based on its configuration. It may be a time-consuming operation
- `is_compliance`: evaluate at run-time if core is global compliant based on its configuration. It may be a very time-consuming operation
- `update_compliant`: use `is_compliant` to update core's state
- `_operation_to_discover`: returns a dictionary `operation_name` => `not_satisfied_need`, based on current connections. This operations should be discover
- `execute`: execute the operation by its name, sending provided data. You must specify which plugin must be used, otherwise `ValueError` is raised.
- `sudo_execute`: bypass all checks and send data to topic

```python
# Create a custom core

@dataclass
class MyCore(Core):
    ...     # core's sinks and logic
```

#### Required operations


`operation_requirements` attribute is a dictionary where keys are operation names and values are `OperationRequirement` objects.

We can specify:

- `constraint`: set of rules to manage connection requests
- `override_sink`: to specify a different sink with respect to default provided by core 
- `default_setup_data`: bytes which are send by default to plugin on connection establishment

`Constraint` class allows you to be very granular in rule specifications:

- `mandatory`: list of needed plugin identifiers
- `minimum` number of additional plugins (plugins in mandatory list are excluded)
- `maximum` number of additional plugins (plugins in mandatory list are excluded)
- `allowlist`/`blocklist`
- `inputs`: represents the list of supported `Input` 
- `outputs`: represents the list of supported `Output` 

In other words, you can specify a list of possible and different inputs and outputs which are supported by your core.

You must observe that inputs and outputs are not related, therefore all possible combinations are evaluated.

For example, if your core needs these operations:

- `operation1`: `"plugin1"` is mandatory, at least 2 additional plugins, maximum 5 additional plugins. Borrowable `operation1` can be fed with "no input", `Operation1InputV1Message` or `Operation1InputV2Message`, instead they produce nothing as output
- `operation2`: `"plugin1"` and `"plugin2"` are allowed (pluggable), there is no a minimum or a maximum number of additional plugins. Borrowable `operation2` must be feedable with empty events (no message) and they must produce `Opeartion2OutputMessage` messages
- `operation3`: no mandatory plugins required, no additional plugins required (any number of connections), but `"plugin2"` can not be plugged (due to `blocklist`). `operation3` has no input (therefore core doesn't interact with plugin, but is plugin to send data arbitrary). `Operation3Output` is expected to be sent to core

```python
YourCore(
    ...,    # other attributes such as eventbus_client, identifier, ... (see next example)
    operation_requirements={

        # required operation name: "operation1"
        "operation1": OperationRequirement(Constraint(
            minimum=2,      # minimum number of non-mandatory plugins
            maximum=5,      # maximum number of non-mandatory plugins
            mandatory=["plugin1"],  # list of mandatory plugins
            inputs=[Input.no_input(), Input.from_message(Operation1InputV1Message), Input.from_message(Operation1InputV2Message)],  # list of supported inputs for this operation
            outputs=[Output.no_output()]    # list of supported outputs for this operation
        )),

        # required operation name: "operation2"
        "operation2": OperationRequirement(Constraint(
            minimum=0,
            allowlist=["plugin1", "plugin2"],   # list of allowed plugins
            inputs=[Input.empty()],
            outputs=[Output.from_message(Opeartion2OutputMessage)]
        )),

        # required operation name: "operation3"
        "operation3": OperationRequirement(Constraint(
            blocklist=["plugin2"]       # list of blocked plugins
            inputs=[Input.no_input()],
            outputs=[Output.from_message(Operation3Output)]
        )),
    }
)
```

#### Sink

In order to be able to handle operation outputs, cores must be equipped with `Sink`, which are basically Busline's `EventHandler` _associated to an operation_.

Operation's sinks are stored in `operation_sinks`. You can manage them in four ways:

- Manually add them directly using the attribute `operation_sinks`

```python
@dataclass
class MyCore(Core):

    # Add in post init a new sink (for operation's name "my_operation")
    def __post_init__(self):
        super().__post_init__()     # mandatory! Otherwise Core logic will not be executed

        # Add a new sink for operation "my_operation", it is a simple in-place lambda
        self.operation_sinks["my_operation"] = CallbackEventHandler(lambda t, e: print(t))
```

- Overriding the pre-defined sink using `override_sink` field in `OperationRequirement` during core instantiation. You must provide a Busline `EventHandler`

```python
core = MyCore(
        eventbus_client=...,    # build a Busline's client
        # ...other attributes
        operation_requirements={
            "lowercase": OperationRequirement(Constraint(
                inputs=[Input.from_message(StringMessage)],
                outputs=[Output.from_message(StringMessage)],

            #  provide a new sink: 
            ), override_sink=CallbackEventHandler(lambda t, e: print(t)))   
            # in-place lambda will be used to handle new events
        }
    )
```

- Using `with_operation_sink` method during core instantiation to add more sinks in core instance (this has the same effect of direct management in `__post_init__`)

```python
core = MyCore(...)      # fill with core attributes
        .with_operation_sink(
            operation_name="my_operation",  # sink's related operation name
            handler=CallbackEventHandler(lambda t, e: print(t))     # event handler for operation's outputs
        )
```

- `@sink` decorator, which you can use to decorator your methods and functions providing *operation name*

```python
@dataclass
class MyCore(Core):     # Inherit Core class to create your custom core

    @sink("plugin_operation_name")  # new sink with related operation name
    async def my_operation_event_handler(self, topic: str, event: Event[MyMessage]):
        ...     # sink logic
```

Sinks in `operation_sinks` are used to link sink automatically with related operation during handshake. Sink related to an operation in `operation_sinks` is ignored if `override_sink` in `OperationRequirement` for that operation is set.




#### Example

You should consider plugins of [this example](#plugins-inheritance-example).

```python
@dataclass
class SmartHomeCore(Core):
    # Dictionary to store lamps statues
    lamp_status: Dict[str, str] = field(default_factory=dict)

    @sink(  # declared sink related to operation "get_status" 
        operation_name="get_status"
    )
    async def get_status_sink(self, topic: str, event: Event[StatusMessage]):
        self.lamp_status[event.payload.lamp_identifier] = event.payload.status  # store plugin status
```

```python
smart_home = SmartHomeCore(
   identifier="smart_home",     # core's identifier
   eventbus_client=...,     # Busline's client
   operation_requirements={

        # required operation name: "turn_on"
        "turn_on": OperationRequirement(Constraint(
            minimum=1,  # required amount of generic plugins
            mandatory=[self.lamp_x_plugin.identifier],  # mandatory plugin (identifier)
            inputs=[Input.empty()],     # list of supported inputs (in this case empty events)
            outputs=[Output.no_output()]    # list of supported outputs (in this case no outputs are expected)
        )),

        # required operation name: "turn_off"
        "turn_off": OperationRequirement(Constraint(
            minimum=1,
            allowlist=[self.lamp_x_plugin.identifier],  # list of pluggable plugins
            inputs=[Input.empty()],
            outputs=[Output.no_output()]
        )),
    }
)
```

> [!TIP]
> Check [Busline documentation](https://github.com/orbitalis-framework/py-busline) to know how to create an eventbus client.


#### Execute an operation

The main capability of a [Core](#core) is **execute plugins operations**. As already mentioned, there are two methods to execute operations:

- `execute`
- `sudo_execute`

##### Execute

`execute` is the regular method to execute an operation which uses connections to choose right plugins.

To execute an operation you must provide:

- `operation_name`
- `data` (*optional*, input of operation)
- *Modality*: one among the following parameters: `all`, `any` or `plugin_identifier` 

In fact, `execute` retrieves current connections related to provided `operation_name`, *evaluating compatibility with data input type*.

> [!WARNING]
> If modality is not provided, `ValueError` is raised.

Then, given the set of all potentially connections, core sends data to topics chosen based on modality:

- `all` sends data to all plugins related to retrieved connections
- `any` sends data to a random plugin related to retrieved connections
- `plugin_identifier` sends data to specified plugin

For example, suppose you want to execute `"plugin_operation"` of plugin `"plugin_identifier"`, sending an empty event (i.e., no data):

```python
await self.my_core.execute("plugin_operation", plugin_identifier="plugin_identifier")
#                           ^^^^^^^^^^^^^^^^ operation's name
```


##### Sudo execute

`sudo_execute` allows to bypass connections, send an execution request to a plugin.

```python
my_core.sudo_execute(
    topic="operation_topic",    # topic on which message will be published
    data=YourMessageData(...)   # message data which will be sent
)
```

> [!IMPORTANT]
> We provide `sudo_execute` method because Orbitalis framework works in secure and managed environment, therefore we think a developer can execute arbitrary operations, even if we advice against to use `sudo_execute`. 





## Advance Guide

In this section we have inserted more details about library implementation which you can use to modify regular behavior.

We suppose that [User Guide](#user-guide) was read before this, because in this section we only add more details. 


### Communication protocols more in deep

#### Handshake

Handshake is the most complex protocol in Orbitalis, it has a lot of possible branches.
The entire process is automatically managed by Orbitalis framework, in fact event handlers are *private* methods, you should use provided hooks (listed in [User Guide](#user-guide)) to change handshake behavior.

Following the enriched handshake sequence diagram: 

```mermaid
sequenceDiagram
    Core->>Plugin: Discover
    Plugin->>Plugin: Update acquaintances
    Plugin->>Plugin: New pending requests
    Plugin->>Core: Offer
    Core->>Core: Update acquaintances
    par Reply for each offered operation
        alt is still needed
            Core->>Core: New pending request
            Core->>Plugin: Request
        else not needed anymore
            Core->>Plugin: Reject
            Plugin->>Plugin: Remove pending request
        end
    end
    par Response for each requested operation
        alt is still available
            Plugin->>Plugin: Promote pending request to connection
            Plugin->>Core: Confirm
            Core->>Core: Promote pending request to connection
        else operation no longer available
            Plugin->>Plugin: Remove pending request
            Plugin->>Core: OperationNoLongerAvailable
            Core->>Core: Remove pending request
        end
    end
```

In the following chapters we will discuss more in deep all steps.

##### Discover

*Discover* message is the first message of the protocol which is sent by [cores](#core). It has two main goals:

- **Notify it existence to all plugins**, providing information such as `core_identifier`, `offer_topic`, `core_keepalive_topic` (topic which plugins must use to send keepalive to core), `core_keepalive_request_topic` (topic which plugins must use to send a keepalive request), `considered_dead_after` (time after which plugins are considered dead if keepalive is not sent)
- **Retrieve operations** thanks to `queries` field, which is a dictionary `operation_name => DiscoverQuery`

`DiscoverQuery` is a dataclass used to store information about operation requirements. Basically it provides constraint information and operation's name.

As already mentioned in [User Guide](#user-guide), you must ensure that `discover_topic` field is equal for all your orbiters, otherwise discover messages will be lost.  

There are more than one methods to send a discover message, but all wrap `send_discover_for_operations` call. In this method:

1. Discover message is published
2. `_last_discover_sent_at` is updated

> [!NOTE]
> Offer topic is fixed and pre-defined by `offer_topic` property, in order to allow a single subscription and future offers. 

##### Offer

When a [plugin](#plugin) receives a discover, it will be processed. In particular, based on plugin's [policy](#policy), if there are some available slots, they are proposed to core thanks to an *offer* message. In particular, `__allow_offer` is used to check compatibility.

First of all, when a new discover event arrives, plugin updates its acquaintances, i.e. updates knowledge about keepalive request topics, keepalive topics and dead time (thanks to `update_acquaintances` method).

Offer logic is mainly present in the discover event handler and in the `send_offer` method.

Actually, `send_offer` can be called in any moment, this allows plugins (and you) to modify a little bit the handshake logic. For example, plugins can send offer messages in a second moment. This feature will be add in a next version.

Similarly to [discover](#discover), even offer messages are used to share information about plugins with cores. In fact, plugins also share `plugin_identifier`, `reply_topic`, `plugin_keepalive_topic` (topic which cores must use to send keepalive to core), `plugin_keepalive_request_topic` (topic which cores must use to send a keepalive request), `considered_dead_after` (time after which cores are considered dead if keepalive is not sent).

In addiction `offered_operations` list is provided. To reduce information sharing, only essential information about plugin's operations are sent, i.e. name, input and output (used to check compatibility).
In fact, remember that cores firstly search operations by name and then check input/output compatibility.

You must know that when a plugin offers a slot, a new [pending request](#pending-requests) is created, in order to preserve that slot for a period of time and wait core response events.


###### Pending Requests

`PendingRequest` contains information about its future [connection](#connections). It is built during [handshake](#handshake) process, so generally you should not use this class.
Anyway, it has `into_connection` method to build a `Connection` and, similarly to connections, has a `lock` attribute to synchronize elaborations.
`created_at` is the field used to check if a pending request must be discarded.

You can manage pending requests thanks to:

- `_pending_requests_by_remote_identifier`: retrieves pending requests based on remote identifier
- `_add_pending_request` 
- `_remove_pending_request` (_does not lock automatically the pending request_)
- `_is_pending` to know if there is a pending request related to a remote identifier and an operation name
- `_promote_pending_request_to_connection`: (_does not lock automatically the pending request_) transforms a pending request into a connection
- `discard_expired_pending_requests` based on `pending_requests_expire_after` and `created_at`


##### Reply

When an offer event arrives, similarly to plugins, it update its acquaintances. Then, core evaluates the offer and for each offered operation it *replies*.

In particular, there are two different possible reply messages:

- **Request** the operation (actually)
- **Reject** the operation if not need anymore (e.g., another plugin offers the same operation before)

> [!NOTE]
> Core responses for each operation instead of using a batch to reduce dimension of messages and allow it to request operations even after some time.

If offered operation is rejected, a simple message `RejectOperationMessage` is sent, in which core's identifier and operation's name are specified.

Instead, if offered operation is still required, then a `RequestOperationMessage` is sent. In that message more details about future connection are provided:

- `output_topic`: topic on which outputs of the operation must be sent (only present if output is acquirable by core)
- `core_side_close_operation_connection_topic`: topic which will be used to send close connection messages
- `setup_data` (in `bytes`): data which will be used to setup the connection

In addiction, also cores generate new pending requests when a request is sent. This allows cores to request more connections at the same time, avoid too many connections risk. 

> [!NOTE]
> Independent from request message type, cores update last seen for plugins when an offer message is received. This a side effect which may reduce number of [keepalive](#keepalive) messages.


##### Response

Finally, the last protocol stage involves a *response* to cores *if needed*.

In particular, if a `RejectOperationMessage` arrives, plugin removes related pending request in order to make the operation slot available again.

Otherwise, if a `RequestOperationMessage` arrives means that core really want to create a connection. In this case there are two possibilities:

- **Operation's slot is still available**
- **Operation's slot is not longer available**

Availability is checked thanks to `__can_lend_to_core` method.

> [!NOTE]
> Similarly to cores, plugins update last seen for cores. Again, this a side effect which may reduce number of [keepalive](#keepalive) messages.

If the slot is still available, then related pending request (plugin-side) is *promoted* to actual connection and `_plug_operation_into_core` method is called. 
This starts plug procedure: last missed and required topics (i.e., `incoming_close_connection_topic` and `operation_input_topic_for_core`) are built, operation is set upped (thanks to `_setup_operation`) 
and finally `ConfirmConnectionMessage` is sent to core, in order to notify it.

When the [core](#core) receives the `ConfirmConnectionMessage` check if confirmation was expected (otherwise ignores it) and promotes core-side the related pending request, links sink of the operation 
and subscribes itself to connection's topics.

> [!WARNING]
> Remember that if you change custom sink for an operation or if you change `override_sink` of an `OperationRequirement`, **already linked sinks will not be modified**. You must close and re-create the connection.

> [!CAUTION]
> If you use unlucky set of parameters for cores and plugins, a confirmation may arrive to core without a related pending request. This may occur when core's pending request expired before `ConfirmConnectionMessage`. This is the main reason behind needs of close unused connections. If a connection is opened plugin-side and core-side it will not open, connection plugin-side expires naturally, **preventing operation's slot starvation**. For this reason you should always provide a timeout for unused connections, avoiding `None` value for `close_connection_if_unused_after` field.

Instead, if operation's slot is not longer available, plugin sends `OperationNoLongerAvailableMessage` to core in order to notify it. In this case, core simply removes related pending request.


### Orbiter

#### Close unused connections

`close_unused_connections` is the Orbiter method which is used to close connections which are untouched during last `close_connection_if_unused_after` (attribute of `Orbiter`) and return the number of sent close request.

This method is called periodically in the Orbiter's main [loop](#loop). As already mentioned, if a connection is unused, then [graceful close procedure](#close-connections) is started.

To prevent this behavior, you can set `close_connection_if_unused_after=None` during orbiter instantiation. In this case, `0` is always returned.

#### Loop implementation

Loop is managed in `__loop` (private) method. The method is private because the entire logic is fully managed and hooks are provided.

Basically, loop is controlled by two `asyncio.Event`:

- `__stop_loop_controller`: used in `while` condition, if it is set, loop is stopped
- `__pause_loop_controller`: if it is set, loop iteration are skipped

As already mentioned in [User Guide](#user-guide), `set` and `clear` method of `asyncio.Event` are wrapped in `stop_loop`, `pause_loop`, `resume_loop` methods.


#### Custom loop

If you don't want to use [regular loop](#loop) into an [orbiter](#orbiter), you can create your custom `asyncio` loop without any strange knowledge about this framework.
Anyway, **we advice against to use custom loops**, because they reduce performance and must be manually managed by you (instead provided loop is already fully managed by us).

Following we provide you an example to show how to create a custom loop. You can notice that compared with [version with a timer](#periodic-operations) explained in the [User Guide](#user-guide) 
(the single different is basically `StringMessage` instead of `Int64Message`), there is a lot of more code, you must know how to use `asyncio.Event` and less features (simil-`pause_loop` method is missed).


```python
@dataclass
class HelloSenderPlugin(Plugin):
    """
    This plugin has only one operation which sends a "hello" message periodically
    """

    __stop_custom_loop: asyncio.Event = field(default_factory=lambda: asyncio.Event())

    def __post_init__(self):
        super().__post_init__()

        # Manually define "hello" operation
        self.operations["hello"] = Operation(
            name="hello",     # operation's name
            input=Input.no_input(),     # no inputs are expected
            handler=None,               # handler is not needed, given that no inputs must be processed
            output=Output.from_message(StringMessage),   # "hello" string will be sent to cores
            policy=Policy.no_constraints()          # no constraint during handshake
        )

    @override
    async def _internal_start(self, *args, **kwargs):
        await super()._internal_start(*args, **kwargs)

        self.__stop_custom_loop.clear()     # allow to start custom loop

        # Start custom loop task
        asyncio.create_task(        # created task is ignored, because __stop_custom_loop is used
            self.__custom_loop()
        )

    @override
    async def _internal_stop(self, *args, **kwargs):
        await super()._internal_stop(*args, **kwargs)

        # During plugin stop, custom loop must be stopped too
        self.__stop_custom_loop.set()

    async def __custom_loop(self):
        while not self.__stop_custom_loop.is_set():
            await asyncio.sleep(2)      # to prevent spamming, custom loop is paused for 2 seconds

            await self.__send_hello()

    async def __send_hello(self):
        connections = await self._retrieve_and_touch_connections(operation_name="hello")  # retrieve current core connections

        tasks = []
        for connection in connections:
            if connection.has_output:   # check if output is expected (it should be...)
                tasks.append(
                    asyncio.create_task(
                        self.eventbus_client.publish(   # send random int
                            connection.output_topic,
                            "hello"     # hello message
                        )
                    )
                )

        await asyncio.gather(*tasks)
```



### Plugin

#### Manual operations management

We have already provided you some examples of how to manage operations manually, anyway we will cover it again.

Firstly, remember [ways to manage operations](#operations). If you really want to manage them manually you can use `operations` field,
which is a dictionary `operation_name => Operation` which contains all operations information of a plugin.

In any moment and in any place in your plugin code you can:

- **Add an operation**:

```python
# Manually define "hello" operation
self.operations["hello"] = Operation(
            name="hello",     # operation's name
            input=Input.no_input(),     # no inputs are expected
            handler=None,               # handler is not needed, given that no inputs must be processed
            output=Output.from_message(StringMessage),   # "hello" string will be sent to cores
            policy=Policy.no_constraints()          # no constraint during handshake
        )
```

- **Modified an operation**:

```python
# e.g., change the policy constraints
self.operations["hello"].policy.allowlist = ["your_core_identifier"]
```

- **Remove an operation**:

```python
# Remove entry from dictionary
del self.operations["hello"]
```

### Core

#### Manual requirements management

In order to modify operation requirements manually, you can perform the same actions explained for [manual operations management](#manual-operations-management), 
considering [core](#core) field `operation_requirements` which contains dictionary `operation_name => OperationRequirement`.

#### Manual sinks management

In order to modify sinks manually, you can perform the same actions explained for [manual operations management](#manual-operations-management), 
considering [core](#core) field `operation_sinks` which contains dictionary `operation_name => EventHandler`.

#### Dynamic operation inputs

Consider the following scenario, you have a core plugged to more plugins for the same operation (e.g., `"save"`) and you want to send a different value based on their input.

In this case you must check manually connection input compatibility with your message's schema.

In the following example we provide you an example in which there are two different plugins which perform the same operation but on different input data, i.e. they store in their internal vault last received value.
Instead `MyCore` has a method `execute_dynamically` which sends `42` if the connection is related with `Int64Message`, instead `"hello"` if input is `StringMessage`.

```python
@dataclass
class SaveIntegerPlugin(Plugin):
    """
    Save inbound integer
    """

    vault: Optional[int] = None

    @operation(
        name="save",  # operation's name
        input=Input.int64(),  # operation's input, i.e. an int number
        output=Output.no_output()  # operation's output, i.e. no outputs
        # no policy is specified => Policy.no_constraints()
    )
    async def save_int_event_handler(self, topic: str, event: Event[Int64Message]):
        self.vault = event.payload.value
```

```python
@dataclass
class SaveStringPlugin(Plugin):
    """
    Save inbound string
    """

    vault: Optional[str] = None

    @operation(
        name="save",  # operation's name
        input=Input.string(),  # operation's input, i.e. a string
        output=Output.no_output()  # operation's output, i.e. no outputs
        # no policy is specified => Policy.no_constraints()
    )
    async def save_int_event_handler(self, topic: str, event: Event[StringMessage]):
        self.vault = event.payload.value
```

```python
@dataclass
class MyCore(Core):

    async def execute_dynamically(self):
        """
        Send right data type based on plugin operations
        """

        # First retrieve all connections related to operation
        connections = self.retrieve_connections(operation_name="save")

        for connection in connections:
            # Ignore connection without an input
            if not connection.has_input:
                continue

            # If connection input has a schema compatible with Int64Message send 42
            if connection.input.is_compatible_with_schema(Int64Message.avro_schema()):
                await self.eventbus_client.publish(
                    connection.input_topic,
                    42
                )

            # If connection input has a schema compatible with StringMessage send "hello"
            if connection.input.is_compatible_with_schema(StringMessage.avro_schema()):
                await self.eventbus_client.publish(
                    connection.input_topic,
                    "hello"
                )
```

```python
int_plugin = SaveIntegerPlugin(
    identifier="int_plugin",
    eventbus_client=build_new_local_client(),
    raise_exceptions=True,
    with_loop=False,
)

str_plugin = SaveStringPlugin(
    identifier="str_plugin",
    eventbus_client=build_new_local_client(),
    raise_exceptions=True,
    with_loop=False,
)

core = MyCore(
    eventbus_client=build_new_local_client(),
    with_loop=False,
    raise_exceptions=True,
    operation_requirements={
        "save": OperationRequirement(Constraint(
            inputs=[Input.int64(), Input.string()],
            outputs=[Output.no_output()],
            mandatory=["int_plugin", "str_plugin"]
        ))
    }
)

await int_plugin.start()
await str_plugin.start()
await core.start()

await asyncio.sleep(2)      # time for handshake

self.assertTrue(core.state == CoreState.COMPLIANT)

await core.execute_dynamically()

await asyncio.sleep(2)  # time to execute

self.assertEqual(int_plugin.vault, 42)
self.assertEqual(str_plugin.vault, "hello")

await int_plugin.stop()
await str_plugin.stop()
await core.stop()

await asyncio.sleep(1)      # time for close connection
```


## Future Work

We have planned to allow Plugins to send an offer in a second moment, as explained in [handshake](#handshake) section. To do this, we are going to modify [Discover message](#discover) to allow Cores to decide if this new feature must be considered thanks to a boolean flag. 

In addiction, we would improve how to change an already linked sink or operation event handler for connections. Now, a connection must be closed and then re-opened.

## Contribute

In order to coordinate the work, please open an issue or a pull request.

**Thank you** for your contributions!


## Authors

### Nicola Ricciardi

Ideator of DHCP-like handshake protocol used by [Orbitalis](#orbitalis). Designer of [Orbitalis](#orbitalis)' protocols and components. Developer of [Orbitalis](#orbitalis)' code base. Documentation referent.

University of Modena and Reggio Emilia.


### Marco Picone

Co-Designer of [Orbitalis](#orbitalis)' main communication [protocols](#communication-protocols-more-in-deep) and components. Documentation reviewer.

Associate Professor at University of Modena and Reggio Emilia.



