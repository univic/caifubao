import sys
import time


def progress_bar():
    """
    this is a closure function
    :return:
    """
    start = time.perf_counter()

    def show_progress_bar(progress, scale):
        bar_width = 0.5
        stop = time.perf_counter()

        percentage = (progress / (scale - 1)) * 100
        a = "*" * int(percentage * bar_width)
        b = "." * int((100 - percentage) * bar_width)
        dur = stop - start
        print(f"\r{progress}/{scale} {percentage:6.2f}%[{a}>{b}]{dur:6.2f}s", end="")

    return show_progress_bar
