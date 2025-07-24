# Orbitalis

Distributed, event-based, micro-kernel library for Python.

## Quick start

```python
@dataclass
class LowercaseTextProcessorPlugin(Plugin):
    @operation(
        name="lowercase",
        input=Input.from_message(StringMessage),
        output=Output.from_message(StringMessage)
    )
    async def lowercase_event_handler(self, topic: str, event: Event[StringMessage]):
        lowercase_text = event.payload.value.lower()

        connections = self._retrieve_connections(input_topic=topic, operation_name="lowercase")

        tasks = []
        for connection in connections:
            if connection.has_output:
                tasks.append(
                    asyncio.create_task(
                        self.eventbus_client.publish(
                            connection.output_topic,
                            lowercase_text
                        )
                    )
                )

        await asyncio.gather(*tasks)
```

```python
@dataclass
class MyCore(Core):
    last_result: Optional[str] = None

    @sink("lowercase")
    async def lowercase_sink(self, topic: str, event: Event[StringMessage]):
        self.last_result = event.payload.value
```

```python
plugin = LowercaseTextProcessorPlugin(
    eventbus_client=PubSubClientBuilder().with_subscriber(LocalSubscriber(eventbus=LocalEventBus())).with_publisher(LocalPublisher(eventbus=LocalEventBus())).build(),
)

core = MyCore(
    eventbus_client=PubSubClientBuilder().with_subscriber(LocalSubscriber(eventbus=LocalEventBus())).with_publisher(LocalPublisher(eventbus=LocalEventBus())).build(),
    needed_operations={
        "lowercase": Need(Constraint(
            inputs=[Input.from_message(StringMessage)],
            outputs=[Output.from_message(StringMessage)],
        ))
    }
)

await plugin.start()
await core.start()

await asyncio.sleep(2)

await core.execute(
    operation_name="lowercase",
    data=StringMessage("HELLO"),
    any=True
)

await asyncio.sleep(1)

assert core.last_result == "hello"

await plugin.stop()
await core.stop()
```

## Documentation

Every communication is asynchronous and uses [Busline](https://github.com/orbitalis-framework/py-busline) to send events.

Every time-based values are expressed in seconds, because `asyncio.sleep` is used.

### Overview

Orbitalis allows you to start cores and plugins, connect them together and execute plugin operations.
Cores and plugins can be started in any time and connections are created based on pre-defined policies.

Messages used by Orbitalis are **Avro messages** because we need input and output schemas.

### Orbiter

`Orbiter` is the base class which provides common capabilities to components.

It manages _pending requests, connections, keepalive and connection close procedure_. In addiction, it has useful _shared methods_ and main _loop_.

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

- `_retrieve_connections`: retrieve all connections which satisfy query
- `discard_expired_pending_requests`: remove expired pending requests and return total amount of discarded requests 
- `close_unused_connections`: send a graceful close request to all remote orbiter if connection was unused based on `close_connection_if_unused_after`
- `update_acquaintances`: update knowledge about keepalive request topics, keepalive topics and dead time
- `have_seen`: update last seen for remote orbiter
- `send_keepalive`
- `send_keepalive_request`
- `send_all_keepalive_based_on_connections`: send keepalive messages to all remote orbiters which have a connection with this orbiter
- `send_keepalive_based_on_connections_and_threshold`: send keepalive messages to all remote orbiters which have a connection with this orbiter only if `send_keepalive_before_timelimit` seconds away from being considered dead this orbiter
- `send_graceless_close_connection`: send a graceless close connection request to specified remote orbiter, therefore, self side connection will be closed immediately
- `send_graceful_close_connection`: send a graceful close connection request to specified remote orbiter, therefore self side connection is not close immediately, but ACK is waited
- `_close_self_side_connection`: close local connection with remote orbiter, therefore only this orbiter will no longer be able to use connection. Generally, a close connection request was sent before this method call.
- `_loop`: contains loop logic (it should not be overridden)


### Plugin

`Plugin` is an `Orbiter` and it is basically an _operations provider_. In a certain sense, plugins lent possibility to execute their operations.

In particular, every plugin has a set of operations which are exposed to other components (i.e., cores).
Only connected components should execute operations.

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

An operation (`Operation`) represents a feature of a plugin, which is exposed and can be executed remotely. 
Operations are managed by `OperationsProviderMixin` which also provides builder-like methods.

Every operation has the folliwing attributes:

- `name`: unique name which identify the operation
- `handler`: [Busline](https://github.com/orbitalis-framework/py-busline) handler, which will be used to handle inbound events
- `policy`: specifies default operation lending rules, you can override this using `with_custom_policy` method or modifying `operations` attribute directly
- `input`: specifies which is the acceptable input
- `output`: specifies which is the sendable output

Even if output can be specified, if a Core doesn't need it, it is not sent.

You can add manually operations to a plugin thanks to `operations` attribute, otherwise you can use `@operation` **decorator**. 

`@operation` if you don't provide an `input` or an `output`, they are considered "no input/output" (see [input/output](#input--output)). 
If you don't specify `default_policy`, `Policy.no_constraints()` is assigned.

For example, if you want to create a plugin having an operation `"lowercase"` which supports strings as input and produces strings (without lent constraints):

```python
@dataclass
class LowercaseTextProcessorPlugin(Plugin):
    @operation(
        name="lowercase",
        input=Input.from_message(StringMessage),
        output=Output.from_message(StringMessage)
    )
    async def lowercase_event_handler(self, topic: str, event: Event[StringMessage]):
        ...
```

> [!NOTE]
> Method name is not related to operation's name.

##### Policy

`Policy` allows you to specify for an operation:

- `maximum` amount of connections
- `allowlist` of remote orbiters
- `blocklist` of remote orbiters

Obviously, you can specify `allowlist` or `blocklist`, not both.

If you don't want constraints: `Policy.no_constraints()`.

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
input = Input.no_input()

assert not input.has_input

output = Output.no_output()

assert not output.has_output
```

If we want to generate a filled `Input`/`Output`:

```python
Input.from_message(MyMessage)

Input.from_schema(MyMessage.avro_schema())

Input.empty()

Input.undefined()

Input(schemas=[...], support_empty_schema=True, support_undefined_schema=False)
```

By default, given that we use Avro JSON schemas, two schemas are compatible if the dictionary version of both is equal or
if the string version of both is equal.

For this reason, you must avoid time-based default value in your classes, because Avro set as default a time-variant value. 
Therefore, in this way, two same-class schema are **different**, even if they are related to the same class.

### Core

#### Needs

#### Sinks

### Connections

#### Pending requests



### Handshake

### Close connections

### Keepalive

### Loop





- Discover fisso e condiviso
- Offer sempre uguale così che un plugin possa offrire senza discover
- Touch connection per logiche di scollegamento
- Reply/Response per singole operation per dare tempo di decidere
- Lock su pending request e connection
- Usare lock su promoto pending request, remove pending request, remove connection

