import os
import configparser

script_path = os.path.dirname(os.path.realpath(__file__))
print(script_path)
config_path = os.path.realpath(script_path + '/data')
print(config_path)
cwd = os.getcwd()

config = configparser.ConfigParser()
# default configs
print(os.path.join(config_path, 'twiddle_defaults.cfg'))
print(os.path.join(cwd, 'twiddle.cfg'))
config.read(os.path.join(config_path, 'twiddle_defaults.cfg'))
config.read(os.path.join(cwd, 'twiddle.cfg'))
