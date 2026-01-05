#!/usr/bin/env python3

import ray
import antithesis.lifecycle as lifecycle
import time

running = True
while(running):
    try:
        ray.init(address='ray://ray-head:10001')
    except Exception as e:
        print(e)
    else:
        lifecycle.setup_complete({})
        print("Ready")
        running = False
    