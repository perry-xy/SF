# @Time : 2021/2/6 10:08 下午 
# @Author : Xingyou Chen
# @File : log.py 
# @Software: PyCharm

import logging
import time
import os
import sys

class Logger():

    def __init__(self, name = '',
                        log_level = logging.INFO,
                        is_console = True,
                        is_file = True):
        self.logger = logging.getLogger(name)
        self.log_level = log_level
        self.is_console = is_console
        self.is_file = is_file
        self.format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        self.file_name = os.path.split(os.path.splitext(sys.argv[0])[0])[-1] + '_' + time.strftime("%Y-%m-%d.log", time.localtime())

        self.logger.setLevel(self.log_level)
        if self.is_console:
            console = logging.StreamHandler()
            console.setFormatter(self.format)
            self.logger.addHandler(console)

        if self.is_file:
            path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'log')
            if not os.path.exists(path):
                os.makedirs(path)

            file = logging.FileHandler(os.path.join(path, self.file_name), encoding="utf-8")
            file.setFormatter(self.format)
            self.logger.addHandler(file)