import time
import datetime


class UpdateWatcher(object):

    def __init__(self):
        pass

    def init_data_update_watcher(self):
        pass

    def data_update_dispatcher(self):
        pass

    def data_update_watcher(self):
        """
        run the scheduled jobs
        :return:
        """

        time_interval = 600  # the time interval to run each check in seconds
        continue_flag = True
        while continue_flag:
            pass
            time.sleep(time_interval)
        pass


data_update_watcher = UpdateWatcher()
