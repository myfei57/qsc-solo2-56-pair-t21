"""门控状态汇总。"""

from __future__ import annotations

from .close import close_durable
from .door import DoorController
from .open import opened
from .queue import command_state, departed, process_queue


def door_state(controller: DoorController, train_id: str) -> dict:
    """汇总一列车的开门、关门、发车与指令队列状态。"""

    executing = controller.commands.executing(train_id)
    return {
        "train_id": train_id,
        "opened": opened(controller, train_id),
        "closed": close_durable(controller, train_id),
        "departed": departed(controller, train_id),
        "queued": len(process_queue(controller, train_id)),
        "executing": executing.id if executing else "",
        "command_state": command_state(controller, train_id)["state"],
    }
