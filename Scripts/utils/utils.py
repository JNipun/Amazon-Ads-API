import logging
import logging.handlers
import json
import os
path=os.getcwd()
def log_handler(class_name):
	logger = logging.getLogger(class_name)
	logger.setLevel(logging.INFO)
	logpath = path+"/Logs/"
	logger_handler = logging.handlers.RotatingFileHandler(
	logpath+'Script_Logs.log', backupCount=10, maxBytes=1000000)
	logger_handler.setLevel(logging.INFO)
	logger_formatter = logging.Formatter('%(name)s - %(levelname)s - '
															'[%(filename)s:%(lineno)s - ' + class_name + '.%(funcName)s] - %(message)s')
	logger_handler.setFormatter(logger_formatter)
	logger.addHandler(logger_handler)
	return logger