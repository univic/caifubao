import logging
import math
import os
import time

logger = logging.getLogger(__name__)


def _use_structured_progress_logs():
    if os.getenv("PROGRESS_LOG_ENABLED", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return True
    log_format = os.getenv("LOG_FORMAT", "json").lower()
    if bool(os.getenv("KUBERNETES_SERVICE_HOST")):
        return False
    return log_format == "json"


def progress_bar():
    """
    this is a closure function
    :return:
    """
    start = time.perf_counter()
    structured_logs = _use_structured_progress_logs()
    last_logged_marker = -1

    def show_progress_bar(progress: int, scale: int, msg: str = ""):
        nonlocal last_logged_marker
        bar_width = 0.5
        stop = time.perf_counter()

        percentage = ((progress + 1) / scale) * 100
        a = "*" * int(percentage * bar_width)
        b = "." * int((100 - percentage) * bar_width)
        dur = stop - start

        average_process_time = dur / (progress + 1)
        estimated_complete_time = average_process_time * scale
        average_process_time_str = get_formatted_duration_str(average_process_time)
        estimated_complete_time_str = get_formatted_duration_str(
            estimated_complete_time
        )
        dur_text = get_formatted_duration_str(dur)
        if structured_logs:
            marker = progress + 1
            log_interval = max(1, min(50, scale // 10 or 1))
            should_log = (
                marker == 1
                or marker == scale
                or marker - last_logged_marker >= log_interval
            )
            if should_log:
                last_logged_marker = marker
                logger.info(
                    "Progress update: current=%s total=%s percent=%.2f duration=%s avg=%s est=%s message=%s",
                    marker,
                    scale,
                    percentage,
                    dur_text,
                    average_process_time_str,
                    estimated_complete_time_str,
                    msg or "-",
                )
            return
        print(
            f"\r{progress + 1}/{scale} | {percentage:6.2f}%|[{a}>{b}]|{dur_text}|Avg{average_process_time_str}|"
            f"Est{estimated_complete_time_str}|{msg}",
            end="",
        )

    return show_progress_bar


def get_formatted_duration_str(dur: float):
    if dur < 60:
        dur_1 = math.floor(dur * 100) / 100
        dur_text = f"{dur_1:.2f}s"
    elif dur < 3600:
        dur_m = int(dur // 60)  # floor divide
        dur_s = int(dur % 60)  # mod calculation
        dur_text = f"{dur_m}m{dur_s}s"
    else:
        dur_h = int(dur // 3600)  # floor divide
        dur_mod_1 = dur % 3600
        dur_m = int(dur_mod_1 // 60)  # floor divide
        dur_s = int(dur_mod_1 % 60)  # mod calculation
        dur_text = f"{dur_h}h{dur_m}m{dur_s}s"
    return dur_text
