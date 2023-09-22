import pytest
import datetime
import time
import pprint
import warnings

time0  = "%s" % datetime.datetime.fromtimestamp( 0 )
utcnow = "%s" % datetime.datetime.utcnow()


def test_001_donkey2_add_dataset_to_rucio():

    """
    Test will
    1. Create a new DB file
    2. Register a dataset with rucio
    3. Provide dbfilename, run number, scope and dsname to downstream tests
    4. Set the dataset to expire in 1/2 hour
    """
    from rucio.client import Client
    client = Client()

    import uuid

    from donkey.donkey_listener import run as run_listener
    from donkey.donkey_dispatch import run as run_dispatch

    from donkey.dataset import dataset_collection as collection

    uu = str( uuid.uuid4() ).replace('-','_')    
    dbfile = 'test-donkey-loop-%s'%uu
    rn=1883

    scope  = 'user.jwebb2'
    dsname = 'test_donkey2_%s_EVENTS-%09i'%(uu,rn)

    pytest.donkey_dbfile    = dbfile
    pytest.donkey_runnumber = rn
    pytest.donkey_scope     = scope
    pytest.donkey_dsname    = dsname
    pytest.donkey_event     = 'create_dts'

    client.add_dataset( scope, dsname )
    client.set_metadata( scope, dsname, 'run_number', str(rn) )

    did = client.get_did( scope, dsname )    
    assert did, "The dataset must be created in rucio"

    client.set_metadata( scope, dsname, 'lifetime',   str(1800) )   # dataset should expire after the test is done

    print("Pausing for 30s...")   
    time.sleep(30)

    # For test 02, loop up to 10 times expect to find event
    pytest.donkey_listen_nloop  = 5
    pytest.donkey_listen_expect = True       


def test_002_donkey2_run_listener():
    """
    Run the listener.  1min loop.  Run up to 10 iterations.
    """

    from donkey.donkey_listener import run as run_listener
    from collections import deque

    # Run the listener and pull the messages into the file
    recieved = False
    count = 0
    while recieved == False and count<pytest.donkey_listen_nloop:
        result = run_listener( [1], pytest.donkey_dbfile, ['pending','dispatched','dropped'] )
        count = count + 1
        for msg in result:
            name  = msg['name']
            event = msg['event']
            if name==pytest.donkey_dsname and event==pytest.donkey_event:
                recieved = True
            else:
                warnings.warn("Listener has not recieved a message after %i min"%count)

    assert recieved==pytest.donkey_listen_expect, "The listener should have recieved a create_dts event for the dataset." 


def test_003_donkey2_dsname_must_be_in_pending():
    """
    The dataset should be available in the pending list
    """

    from donkey.dataset import dataset_collection as collection    

    coll = collection(pytest.donkey_dbfile)
    assert coll.find('pending',pytest.donkey_dsname), 'dataset %s must exist in pending collection after created in rucio'%pytest.donkey_dsname
    

def test_004_donkey2_run_listener():
    """
    Rerun the listener for one more iteration
    """
    pytest.donkey_listen_nloop  = 1
    pytest.donkey_listen_expect = False
        
    test_002_donkey2_run_listener()

def test_005_donkey2_dsname_must_be_in_pending():
    """
    Dataset must still be pending after one more llop iteration
    """
    test_003_donkey2_dsname_must_be_in_pending()

def test_006_donkey2_setup_and_run_dispatch():
    from donkey.donkey_dispatch import run as run_dispatch

    # Setup and run dispatch
    run_dispatch([
        '--dbfile', '%s'%pytest.donkey_dbfile,
        '--rule',   'raw-events',
        '--event',  'closed',
        '--scope',  'user.jwebb2',
        '--actor',  'tests/hello.sh',
        '--regex',  r'(\w)+EVENTS-(\d{9})',
        '--run'
        ])

def test_007_donkey2_run_listener():
    """
    Run the listener loop... should still be in pending..."
    """
    pytest.donkey_listen_nloop  = 1
    pytest.donkey_listen_expect = False # nothing expected from active mq
    test_002_donkey2_run_listener()

def test_008_donkey2_dsname_must_be_in_pending():
    """
    Dataset must still be pending b/c dataset was not closed
    """
    test_003_donkey2_dsname_must_be_in_pending()

def test_009_donkey2_close_dataset():
    from rucio.client import Client
    client = Client()
    client.close( pytest.donkey_scope, pytest.donkey_dsname )
    
def test_010_donkey2_run_listener():
    """
    Run the listener loop... to catch the close event
    """
    pytest.donkey_listen_nloop  = 5
    pytest.donkey_listen_expect = True
    pytest.donkey_event     = 'close'
    
    test_002_donkey2_run_listener()    

def test_011_donkey2_setup_and_run_dispatch():
    from donkey.donkey_dispatch import run as run_dispatch

    # Setup and run dispatch
    run_dispatch([
        '--dbfile', '%s'%pytest.donkey_dbfile,
        '--rule',   'raw-events',
        '--event',  'closed',
        '--scope',  'group.sphenix',
        '--actor',  'tests/hello.sh',
        '--regex',  r'(\w)+EVENTS-(\d{9})',
        '--rule',   'raw-events',
        '--event',  'closed',
        '--scope',  'user.jwebb2',
        '--actor',  'tests/hello.sh',
        '--regex',  r'(\w)+EVENTS-(\d{9})',
        '--run'
        ])


def test_012_donkey2_dsname_must_be_in_dispatched():
    """
    The dataset should be available in the dispatched
    """

    from donkey.dataset import dataset_collection as collection    

    coll = collection(pytest.donkey_dbfile)
    assert coll.find('dispatched',pytest.donkey_dsname), 'dataset %s must exist in dispatched collection after dispatch called'%pytest.donkey_dsname    

    


#____________________________________________________________
def test_900_cleanup():
    from rucio.client import Client
    client = Client()
    #client.erase( pytest.donkey_scope, pytest.donkey_dsname )
    client.set_metadata( pytest.donkey_scope, pytest.donkey_dsname, 'lifetime',   str(1800) )   # dataset should expire after the test is done    


def Xtest_donkey2_loop():
    """
    """
    from rucio.client import Client
    client = Client()

    import uuid

    from donkey.donkey_listener import run as run_listener
    from donkey.donkey_dispatch import run as run_dispatch

    from donkey.dataset import dataset_collection as collection

    uu = str( uuid.uuid4() ).replace('-','_')    
    dbfile = 'test-donkey-loop-%s'%uu
    rn=1883

    scope  = 'user.jwebb2'
    dsname = 'test_donkey2_%s_EVENTS-%09i'%(uu,rn)

    client.add_dataset( scope, dsname )
    client.set_metadata( scope, dsname, 'run_number', str(rn) )

    did = client.get_did( scope, dsname )    
    assert did, "The dataset must be created in rucio"    
    client.set_metadata( scope, dsname, 'lifetime',   str(1800) )   # dataset should expire after the test is done

    
    # Run the listener and pull the messages into the file
    run_listener( [5,4,3,2,1], dbfile, ['pending','dispatched','dropped'] )

    coll = collection(dbfile)
    assert coll.find('pending',dsname), 'dataset %s must exist in pending collection after created in rucio'%dsname

    # Setup and run dispatch
    run_dispatch([
        '--dbfile', '%s'%dbfile,
        '--rule',   'raw-events',
        '--event',  'closed',
        '--scope',  'group.sphenix',
        '--actor',  'tests/hello.sh',
        '--regex',  r'(\w)+EVENTS-(\d{9})',
        '--rule',   'raw-events',
        '--event',  'closed',
        '--scope',  'user.jwebb2',
        '--actor',  'tests/hello.sh',
        '--regex',  r'(\w)+EVENTS-(\d{9})',
        '--run'
        ])

    #coll = collection(dbfile)
    assert coll.find('pending',dsname), 'dataset %s must still exist in pending collection after dispatcher looks for closed datasets'%dsname

    # Close the dataset
    client.close( scope, dsname )

    meta_ = client.get_metadata( scope, dsname )
    
    assert( meta_['is_open'] == False ), "dataset %s should have been closed"%dsname

    # Run the listener for 1 min and pull the messages into the file
    run_listener( [5,4,3,2,1], dbfile, ['pending','dispatched','dropped'] )

    coll = collection(dbfile)
    assert coll.find('dispatched',dsname), 'dataset %s should have been dispatched'%dsname    
    
    
