"""
Configuration
"""
from configobj import ConfigObj
import shutil
import os,sys
from cosmos.utils.helpers import confirm

user_home_path = os.path.expanduser('~')
cosmos_path = os.path.join(user_home_path,'.cosmos/')
config_path = os.path.join(cosmos_path,'config.ini')
cosmos_library_path = os.path.dirname(os.path.realpath(__file__))
default_config_path = os.path.join(cosmos_library_path,'default_config.ini')

if not os.path.exists(config_path):
    if confirm('No configuration file exists, would you like to create a default one in {0}?'.format(config_path),default=True):
        if not os.path.exists(os.path.dirname(config_path)):
            os.mkdir(os.path.dirname(config_path))
        shutil.copyfile(default_config_path,config_path)
        print >> sys.stderr, "Done.  Before proceeding, please edit {0}".format(default_config_path)
    else:
        sys.exit(1)
# Creating settings dictionary
co = ConfigObj(config_path)
settings = co.dict()
settings['cosmos_library_path'] = cosmos_library_path
settings['cosmos_path'] = cosmos_path
settings['config_path'] = config_path
settings['user_home_path'] = user_home_path

# Defaults
settings.setdefault('working_directory','/tmp')