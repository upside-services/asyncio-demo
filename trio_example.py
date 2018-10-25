# Taken from https://gist.github.com/njsmith/1c3742362bb6f5cc505f69cb66db2e71
# Ouput seems to be different though; and correct!

import sys
import trio

def dump_stack(where):
    print("-- {} --".format(where))
    frame = sys._getframe(1)
    while frame:
        print(frame.f_code.co_name)
        frame = frame.f_back

async def f():
    dump_stack("f before yielding")
    await g()
    dump_stack("f after yielding")

async def g():
    dump_stack("g before yielding")
    await h()
    dump_stack("g after yielding")

async def h():
    dump_stack("h before yielding")
    await trio.sleep(0)
    dump_stack("h after yielding")

trio.run(f)