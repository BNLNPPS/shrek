import yaml
import os
from os.path import exists

def readSiteConfig(default='site.yaml'):

    shreksys = os.environ.get( 'SHREK_SYS', '.' )      # shrek system installation (defaults to pwd)
    
    configs = [ '%s'%default,                             # first try local file
                'shrek/config/site.yaml',                 # next local install
                '%s/shrek/config/site.yaml'%shreksys]     # finally system install

    config = ""

    for c in configs:
        if exists(c):
            config = c
            break

    print("SHREK configuration found at %s"%config)

    result = {}
    
    with open(config, "r") as stream:
        try:
            result = yaml.safe_load(stream)            
        except yaml.YAMLError as exc:
            print(exc)

    return result

    
             
