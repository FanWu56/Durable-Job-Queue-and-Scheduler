import time
from typing import Any
import random

############  This python file is provide some simulated tasks to test the project

def send_email(payload: dict[str, Any]):
    print(f"Sending email with payload: {payload}")
    time.sleep(1)


def resize_image(payload: dict[str, Any]):
    print(f"Resizing image with payload: {payload}")
    time.sleep(1)

def unstable_task(payload: dict[str, Any]):
    print(f"Running unstable task with payload: {payload}")
    time.sleep(1)

    fail_rate = float(payload.get("fail_rate", 0.5))

    if random.random() < fail_rate:
        raise RuntimeError("unstable_task failed randomly")
    
TASKS = {
    "send_email": send_email,
    "resize_image": resize_image,
    "unstable_task": unstable_task,
}

def execute_task(task_name: str, payload: dict[str, Any]):
    task = TASKS.get(task_name)

    if task is None:
        print(f"No registered task named '{task_name}', treating it as a no-op.")
        print(f"Payload: {payload}")
        time.sleep(1)
        return

    task(payload)

