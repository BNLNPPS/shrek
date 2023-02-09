import yaml
import argparse
import shutil
import datetime
import sh
import time
import stomp
import uuid
import os

donkey = sh.Command("shrek/scripts/DonkeyApplication.py").bake("--user donkey --password donkey")

def test_donkey_import_app():
    """
    We should be able to import the application
    """
    import shrek.scripts.DonkeyApplication


# Test donkey initialization ----------------------------------------------
"""
A new instance of the donkey application should connect to active mq
with a subscription ID.  By default, the subscription ID will be read
in from .donkey/subscription id.
"""

def donkey_config_dir():
    from shrek.scripts.DonkeyApplication import readConfig
    donkeycfg = readConfig()['config']
    return donkeycfg
    
def setup_config_dir():
    """
    Moves existing donkey directory out of the way    
    """
    cfg = donkey_config_dir()
    if os.path.exists( cfg ):
        sh.mv( [ cfg, cfg + "_saved"] )

def restore_config_dir():
    """
    Restores donkey directory at the end of the run
    """
    cfg = donkey_config_dir()
    if os.path.exists( cfg ):
        sh.rm( ['-r', cfg ] )
    sh.mv( [ cfg + "_saved", cfg] )       
    pass        

def test_donkey_setup_config_dir():
    """
    Does not test any functionality.  Simply moves .donkey out of the way.
    """
    setup_config_dir()

# -- BEGIN actual tests -------------

def test_donkey_subscription_id():
    cfg = donkey_config_dir()
    assert  not os.path.exists(cfg),                          "could not configure a clean test setup"

    donkey()
    assert os.path.exists(cfg),      "donkey should create the .donkey file on invocation [did not]"

    expected = [ "subscription-id" ]
    for f in expected:
        assert os.path.exists( cfg + "/" + f ),           "donkey should create " + f + " [did not]"

    subid = ""
    with open( cfg + "/subscription-id" ) as s:
        subid = s.readline()

    assert len(subid)==32,                         "subscription id should be a uuid [is not or missing]"

    donkey()
    
    subid2 = ""
    with open( cfg + "/subscription-id" ) as s:
        subid2 = s.readline()

    assert subid == subid2,             "cached subscription id is loaded on subsequent calls [was not]"

    donkey( " --subscription-id 12345" )

    subid3 = ""
    with open( cfg + "/subscription-id" ) as s:
        subid3 = s.readline()

    assert subid3 == subid, "a user-specified subscription id should not overwrite the cached file [it did]"


# -- END actual tests ---------------

def test_donkey_restore_config_dir():
    """
    Restores the donkey config dir...
    """    
    restore_config_dir()
# -------------------------------------------------------------------------
