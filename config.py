import os
import configparser

script_path = os.path.dirname(os.path.realpath(__file__))
config_path = os.path.realpath(script_path + '/config')
cwd = os.getcwd()

config = configparser.ConfigParser()
# default configs
config.read(os.path.join(config_path, 'twiddle_defaults.cfg'))
config.read(os.path.join(cwd, 'twiddle.cfg'))
