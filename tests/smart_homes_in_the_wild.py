import random
from typing import List
import asyncio
import logging
from busline.local.eventbus.local_eventbus import LocalEventBus
from orbitalis.core.requirement import OperationRequirement, Constraint
from orbitalis.orbiter.schemaspec import Input, Output
from orbitalis.plugin.operation import Policy
from tests.core.smarthome_core import SmartHomeCore
from tests.plugin.lamp.lamp_plugin import LampPlugin, StatusMessage
from tests.plugin.lamp.lamp_x_plugin import LampXPlugin
from tests.plugin.lamp.lamp_y_plugin import TurnOnLampYMessage, TurnOffLampYMessage, LampYPlugin
from tests.utils import build_new_local_client

random.seed(42)
logging.basicConfig(level="INFO")

LOOP_INTERVAL = 1
REPORT_EVERY_N_ITERATIONS = 5
EVENT_BUS = LocalEventBus()
N_CORES = 3
N_PLUGINS = 10

def build_plugin_identifier(i: int):
    return f"plugin-{i}"

def build_core_identifier(i: int):
    return f"core-{i}"

def get_cores() -> List[SmartHomeCore]:
    cores: List[SmartHomeCore] = []

    for i in range(N_CORES):
        cores.append(
            SmartHomeCore(
                identifier=build_core_identifier(i),
                eventbus_client=build_new_local_client(),
                raise_exceptions=True,
                with_loop=bool(random.randint(0,1)),
                operation_requirements={
                    "turn_on": OperationRequirement(Constraint(
                        minimum=random.randint(0, 2),
                        mandatory=[build_plugin_identifier(random.randint(0, N_PLUGINS - 1))],
                        inputs=[Input.empty(), Input.from_schema(TurnOnLampYMessage.avro_schema())],
                        outputs=[Output.no_output()],
                    )),
                    "turn_off": OperationRequirement(
                        Constraint(
                            minimum=random.randint(0, 2),
                            mandatory=[build_plugin_identifier(random.randint(0, N_PLUGINS - 1))],
                            inputs=[Input.empty(), Input.from_schema(TurnOffLampYMessage.avro_schema())],
                            outputs=[Output.no_output()],
                        )
                    ),
                    "get_status": OperationRequirement(
                        Constraint(
                            minimum=0,
                            inputs=[Input.empty()],
                            outputs=[Output.from_schema(StatusMessage.avro_schema())]
                        )
                    )
                }
            )
        )

    return cores

def get_plugins() -> List[LampPlugin]:
    plugins: List[LampPlugin] = []

    for i in range(N_PLUGINS):
        blocklist = None
        # if random.randint(0, 100) < 50:
        #     blocklist = [build_core_identifier(random.randint(0, N_CORES - 1))]

        if random.randint(0, 100) < 50:
            plugins.append(
                LampXPlugin(
                    identifier=build_plugin_identifier(i),
                    eventbus_client=build_new_local_client(),
                    raise_exceptions=True,
                    with_loop=bool(random.randint(0,1)),

                    kw=random.randint(1, 50)
                ).with_custom_policy(
                    operation_name="turn_on",
                    policy=Policy(blocklist=blocklist)
                ).with_custom_policy(
                    operation_name="turn_off",
                    policy=Policy(blocklist=blocklist)
                )
            )
        else:
            plugins.append(
                LampYPlugin(
                    identifier=build_plugin_identifier(i),
                    eventbus_client=build_new_local_client(),
                    raise_exceptions=True,
                    with_loop=bool(random.randint(0, 1)),

                    kw=random.randint(1, 50)
                ).with_custom_policy(
                    operation_name="turn_on",
                    policy=Policy(blocklist=blocklist)
                ).with_custom_policy(
                    operation_name="turn_off",
                    policy=Policy(blocklist=blocklist)
                )
            )

    return plugins


async def main():
    cores = get_cores()
    plugins = get_plugins()

    tasks = []
    for orbiter in [*cores, *plugins]:
        tasks.append(
            asyncio.create_task(orbiter.start())
        )

    await asyncio.gather(*tasks)

    await asyncio.sleep(2)

    iterations = 0
    executions = 0
    while True:
        iterations += 1

        core = random.choice(cores)
        p = random.choice(plugins)

        # if random.randint(0, 100) < 50:
        #     await core.start()
        # else:
        #     await core.stop()
        #
        # if random.randint(0, 100) < 50:
        #     await p.start()
        # else:
        #     await p.stop()

        # === TURN_ON ===
        core = random.choice(cores)

        message = None
        if random.randint(0, 100) < 50:
            message = TurnOnLampYMessage(power=random.random())

        if random.randint(0, 100) < 50:
            executions += await core.execute(
                "turn_on",
                message,
                any=True
            )
        else:
            executions += await core.execute(
                "turn_on",
                message,
                all=True
            )

        # === TURN_OFF ===
        core = random.choice(cores)

        message = None
        if random.randint(0, 100) < 50:
            message = TurnOffLampYMessage(reset_consumption=bool(random.randint(0,1)))

        if random.randint(0, 100) < 50:
            executions += await core.execute(
                "turn_off",
                message,
                any=True
            )
        else:
            executions += await core.execute(
                "turn_off",
                message,
                all=True
            )

        # === GET_STATUS ===
        core = random.choice(cores)

        executions += await core.execute(
            "get_status",
            any=True
        )


        await asyncio.sleep(LOOP_INTERVAL)


        # === SIMULATION REPORT ===
        if iterations % REPORT_EVERY_N_ITERATIONS != 0:
            continue

        print()
        print("=====> TOTAL EVENTS <=====")

        print(f"Unique events: {EVENT_BUS.events_counter}")
        print(f"Executions: {executions}")

        print()
        print("=====> PLUGINS <=====")

        for p in plugins:
            print(f"PLUGIN: {p.identifier}")
            print(f"Status: {p.status}")
            print(f"State {p.state}")
            print()

        print()
        print("=====> CORES <=====")

        for c in cores:
            print(f"CORE: {c.identifier}")
            print(f"State {c.state}")
            print(f"Lamp status: {c.lamp_status}")
            print()


if __name__ == '__main__':
    asyncio.run(main())





