import pytest
import time
import pprint

def test_001_add_dataset_to_rucio_with_1min_lifetime():
    from rucio.client import Client
    client = Client()

    import uuid
    uu = str( uuid.uuid4() )

    pytest.scope  = 'user.jwebb2'
    pytest.dsname = 'test-donkey2-%s'%(uu)
    pytest.limit  = 30

    client.add_dataset( pytest.scope, pytest.dsname )
    client.set_metadata( pytest.scope, pytest.dsname, 'lifetime',   str(60) )  

    did = client.get_did( pytest.scope, pytest.dsname )    
    assert did, "The dataset should have been added to rucio with a 60s lifetime"

def test_002_the_dataset_should_still_exist_immediately_after_creation():
    from rucio.client import Client
    client = Client()



    did = client.get_did( pytest.scope, pytest.dsname )    
    assert did, "The dataset should still be here right after the creation"

def test_003_the_dataset_should_be_removed_in_a_reasonable_time_frame():
    from rucio.client import Client
    client = Client()

    from rucio.common.exception import DataIdentifierNotFound    

    removed=False
    for i in range(0, pytest.limit):
        did = None
        try:
            did = client.get_did( pytest.scope, pytest.dsname )           
            pprint.pprint(did)
            time.sleep(60)            
        except DataIdentifierNotFound:
            removed = True
            print("Removed after %i min"%i)
            break

    assert(removed), "The dataset was not removed int the requested time limit %i min"%pytest.limit


