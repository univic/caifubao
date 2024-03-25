import math
import time


def progress_bar():
    """
    this is a closure function
    :return:
    """
    start = time.perf_counter()

    def show_progress_bar(progress: int, scale: int, msg: str = ""):
        bar_width = 0.5
        stop = time.perf_counter()

        percentage = ((progress + 1) / scale) * 100
        a = "*" * int(percentage * bar_width)
        b = "." * int((100 - percentage) * bar_width)
        dur = stop - start
        dur_text = get_formatted_duration_str(dur)
        print(f"\r{progress + 1}/{scale} {percentage:6.2f}%[{a}>{b}]{dur_text}. {msg}", end="")

    return show_progress_bar


def get_formatted_duration_str(dur: float):
    if dur < 60:
        dur_1 = math.floor(dur * 100) / 100
        dur_text = f'{dur_1:.2f}s'
    elif dur < 3600:
        dur_m = int(dur // 60)  # floor divide
        dur_s = int(dur % 60)  # mod calculation
        dur_text = f'{dur_m}m{dur_s}s'
    else:
        dur_h = int(dur // 3600)  # floor divide
        dur_mod_1 = dur % 3600
        dur_m = int(dur_mod_1 // 60)  # floor divide
        dur_s = int(dur_mod_1 % 60)  # mod calculation
        dur_text = f'{dur_h}h{dur_m}m{dur_s}s'
    return dur_text
