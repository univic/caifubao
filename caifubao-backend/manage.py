# -*- coding: utf-8 -*-
# Author : univic
# Date: 2022-02-13


from app import create_app
from app.utilities.logger import create_logger

logger = create_logger()


if __name__ == '__main__':
    app = create_app()
    # app.run()
    pass

