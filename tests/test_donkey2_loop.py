import pytest
import datetime
import time
import pprint
import warnings

time0  = "%s" % datetime.datetime.fromtimestamp( 0 )
utcnow = "%s" % datetime.datetime.utcnow()


def test_0010_donkey2_add_dataset_to_rucio():

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

    client.set_metadata( scope, dsname, 'lifetime',   str(7200) )   # dataset should expire in 2h even if test crashses
    
    time.sleep(30)

    # For test 02, loop up to 10 times expect to find event
    pytest.donkey_listen_nloop  = 5
    pytest.donkey_listen_expect = True       


def test_0020_donkey2_run_listener():
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


def test_0030_donkey2_dsname_must_be_in_pending():
    """
    The dataset should be available in the pending list
    """

    from donkey.dataset import dataset_collection as collection    

    coll = collection(pytest.donkey_dbfile)
    assert coll.find('pending',pytest.donkey_dsname), 'dataset %s must exist in pending collection after created in rucio'%pytest.donkey_dsname
    

def test_0040_donkey2_run_listener():
    """
    Rerun the listener for one more iteration
    """
    pytest.donkey_listen_nloop  = 1
    pytest.donkey_listen_expect = False
        
    test_0020_donkey2_run_listener()

def test_0050_donkey2_dsname_must_be_in_pending():
    """
    Dataset must still be pending after one more llop iteration
    """
    test_0030_donkey2_dsname_must_be_in_pending()

def test_0060_donkey2_setup_and_run_dispatch():
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

def test_0070_donkey2_run_listener():
    """
    Run the listener loop... should still be in pending..."
    """
    pytest.donkey_listen_nloop  = 1
    pytest.donkey_listen_expect = False # nothing expected from active mq
    test_0020_donkey2_run_listener()

def test_0080_donkey2_dsname_must_be_in_pending():
    """
    Dataset must still be pending b/c dataset was not closed
    """
    test_0030_donkey2_dsname_must_be_in_pending()

def test_0090_donkey2_close_dataset():
    from rucio.client import Client
    client = Client()
    client.close( pytest.donkey_scope, pytest.donkey_dsname )
    
def test_0100_donkey2_run_listener():
    """
    Run the listener loop... to catch the close event
    """
    pytest.donkey_listen_nloop  = 5
    pytest.donkey_listen_expect = True
    pytest.donkey_event     = 'close'
    
    test_0020_donkey2_run_listener()    

def test_0110_donkey2_setup_and_run_dispatch():
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


def test_0120_donkey2_dsname_must_be_in_dispatched():
    """
    The dataset should be available in the dispatched
    """

    from donkey.dataset import dataset_collection as collection    

    coll = collection(pytest.donkey_dbfile)
    assert coll.find('dispatched',pytest.donkey_dsname), 'dataset %s must exist in dispatched collection after dispatch called'%pytest.donkey_dsname    

    


#____________________________________________________________

def test_0130_donkey2_cleanup_the_dataset_and_verify_it_was_removed_from_rucio():
    from rucio.client import Client
    from rucio.common.exception import DataIdentifierNotFound
    
    client = Client()
    client.set_metadata( pytest.donkey_scope, pytest.donkey_dsname, 'lifetime',   str(15) )   # dataset should expire after the test is done

    removed=False
    for i in range(0, 10): # 5 min wait period
        did = None
        try:
            did = client.get_did( pytest.donkey_scope, pytest.donkey_dsname )           
            time.sleep(30)            
        except DataIdentifierNotFound:
            removed = True
            break

    assert(removed), "The dataset was not removed int the requested time limit %i min"%pytest.limit    

    

def test_0140_donkey2_listener_should_see_the_erase_message():
    from donkey.donkey_listener import run as run_listener    
    from collections import deque

    duration = 30 # minutes
    
    recieved = False
    count = 0
    while recieved == False and count < duration:
        result = run_listener( [1], pytest.donkey_dbfile, ['pending','dispatched','dropped'] )
        count = count + 1
        for msg in result:
            name  = msg['name']
            event = msg['event']
            if name==pytest.donkey_dsname and event=='erase':
                recieved = True

    assert recieved, "Did not see an erase event in %i min"%count

def test_0150_donkey2_dataset_should_be_removed_from_dbfile_after_an_erase():
    from donkey.dataset import dataset_collection as collection
    coll = collection(pytest.donkey_dbfile)
    assert coll.find('pending',pytest.donkey_dsname)==None, 'dataset %s must not exist in pending collection after erase'%pytest.donkey_dsname
    assert coll.find('dispatched',pytest.donkey_dsname)==None, 'dataset %s must not exist in dispatched collection after erase'%pytest.donkey_dsname
    assert coll.find('dropped',pytest.donkey_dsname)==None, 'dataset %s must not exist in dispatched collection after erase'%pytest.donkey_dsname            

