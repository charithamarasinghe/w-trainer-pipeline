import logging
import configparser
import os

base_path = os.getcwd()

logging.basicConfig(filename=base_path + '/logs/pipeline.log',
                    filemode='a+',
                    format="%(asctime)s %(levelname)s: %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")

logger = logging.getLogger()
logger.setLevel(logging.WARNING)

config_params = configparser.ConfigParser()
config_params.read(base_path + "/config.ini")
