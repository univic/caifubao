#!/usr/bin/env python
# -*- coding: utf-8 -*-

from app.lib.datahub import Datahub
import sys

if __name__ == "__main__":
    instance = Datahub()

    if len(sys.argv) > 1 and sys.argv[1] == "--scheduled":
        instance.start_scheduled()
        try:
            import time

            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            instance.stop_scheduled()
    else:
        instance.start()
