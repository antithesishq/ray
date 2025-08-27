#!/usr/bin/env python3

import ray
import time

ray.init(address='ray://ray-head:10001')

@ray.remote
def data():
    return 10

@ray.remote(max_task_retries=-1)
class Actor:
    def run(self, x):
        return True


print("actor task starting...")

a = Actor.remote()

in_ref = data.remote()

ref = a.run.remote(in_ref)

time.sleep(1)

ray.cancel(ref)

# Wait for a while, and the process will exit due to RAY_CHECK failed.
time.sleep(60)

print("actor task done")
