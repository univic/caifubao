import sys
import time


def progress_bar(progress, scale):
    start = time.perf_counter()
    a = "*" * progress
    b = "." * (scale - progress)
    c = (progress / scale) * 100
    dur = time.perf_counter() - start
    print("\r{:^3.0f}%[{}->{}]{:.2f}s".format(c, a, b, dur), end="")
