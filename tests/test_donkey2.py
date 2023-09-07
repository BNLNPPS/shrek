import pytest
import datetime
time0  = "%s" % datetime.datetime.fromtimestamp( 0 )
utcnow = "%s" % datetime.datetime.utcnow()
#

def skip_test_donkey2_listener_loop():

    from donkey.donkey_listener import run
    from donkey.dataset import dataset_collection as collection
    from donkey.dataset import dataset
    
    from rucio.client import Client
    import uuid
    
    client = Client()

    uu = str(uuid.uuid4())

    dbfile='test-donkey-listener-%s'%uu

    # create a dataset
    scope='user.jwebb2'
    name='test-donkey2-%s'%uu

    client.add_dataset( scope, name )

    # run the listener for 1min
    run([1],dbfile)
    coll = collection(dbfile)
    assert coll.find('create_dts',name), 'dataset %s does not exist after created in rucio'

    # run the listener for 1min
    run([1],dbfile)    
    assert coll.find('create_dts',name), 'dataset %s does not exist after a second pass of the listening loop'
    

def test_donkey2_import_dataset():
    """
    Import the dataset class
    """
    import donkey.dataset

def test_donkey2_init_dataset():
    """
    Test initialization of a dataset
    """
    from donkey.dataset import dataset

    d = dataset()
    assert d.name       == "" ,    "name should be initialized to empty string"
    assert d.runnumber  == 0,      "should start with an invalid run number"
    assert d.event      == "none", "event should be initialized to none b/c nothing has happened"
    assert d.created    == time0,  "created/closed/dispatch should be initialized to %s"%time0
    assert d.closed     == time0,  "created/closed/dispatch should be initialized to %s"%time0
    assert d.dispatched == time0,  "created/closed/dispatch should be initialized to %s"%time0
    assert d.account    == "none", "account should be intialized to none"
    assert d.scope      == "none", "scope should be initialized to none"
    assert d.bytes      == "nan",  "size of the message defaults to nan"
    assert d.length     == "nan",  "length of the message defaults to nan"

def test_donkey2_init_dataset_collection():
    """
    Test initialization of a dataset collection
    """
    from donkey.dataset import dataset_collection

    n = "testcoll"
    t = "Test collection"
    f = "test_donkey2_init_dataset_collection"
    
    coll = dataset_collection(f,n,t)

    assert coll.db.exists( n ), "The db file should contain a named list"
    assert coll.db.exists( n+".meta" ), "The db file should contain a metadata entry for the list"

    len1 = len( coll.db.getall() )

    coll = dataset_collection(f,n,t)

    len2 = len( coll.db.getall() )

    assert( len1==len2 ), "Multiple creation of the same dataset with same key allowed and results in single key/list created"

def test_donkey2_add_pop_dataset_collection():

    from donkey.dataset import dataset_collection as collection
    from donkey.dataset import dataset

    ds = dataset()
    ds.name      = "dataset1"
    ds.runnumber = 12345
    ds.event     = "test_add"
    ds.created   = str(datetime.datetime.utcnow())

    co = collection( "test_donkey2_add_get_remove_dataset_collection", "new", "Newly created datasets" )
    co = collection( "test_donkey2_add_get_remove_dataset_collection", "processing", "Data sets in process" )
    co = collection( "test_donkey2_add_get_remove_dataset_collection", "done", "Data sets processed" )

    assert len(co.db.getall())==3*2, "There should be three lists and three meta data in the collection"
    assert co.db.exists( "new" ),   "There should be a new"
    assert co.db.exists( "processing" ),   "There should be a processing"
    assert co.db.exists( "done" ),   "There should be a done"

    assert co.length( "new" ) == 0, "The new list should be empty"
    assert co.length( "processing" ) == 0, "The processing list should be empty"
    assert co.length( "done" ) == 0, "The done list should be emptry"

    for n in ["new","processing","done"]:
        co.add( n, ds )
        assert co.length( n ) == 1, "After adding a dataset the size of the %s list shoudl be incremented"%n
        co.pop( n )
        assert co.length( n ) == 0, "The %s list should be empty after a pop"    %n        

    for n in ["new","processing","done"]:  # popping an empty list on a collection is a no-no...
        with pytest.raises(IndexError):
            co.pop( n )

def test_donkey2_add_remove_dataset_collection():

    from donkey.dataset import dataset_collection as collection
    from donkey.dataset import dataset
    
    d1 = dataset()
    d1.name      = "dataset1"
    d1.runnumber = 12345
    d1.event     = "test_add"
    d1.created   = str(datetime.datetime.utcnow())

    d2 = dataset()
    d2.name      = "dataset2"
    d2.runnumber = 12345
    d2.event     = "test_add"
    d2.created   = str(datetime.datetime.utcnow())

    d3 = dataset()
    d3.name      = "dataset3"
    d3.runnumber = 12345
    d3.event     = "test_add"
    d3.created   = str(datetime.datetime.utcnow())

    coll = collection( "test_donkey2_add_remove_dataset_collection" )
    coll.addlist('new', 'addition/removal test')

    coll.add('new',d1)
    coll.add('new',d2)
    coll.add('new',d3)

    assert coll.exists('new',d1), 'Dataset 1 must exist after addition'
    assert coll.exists('new',d2), 'Dataset 2 must exist after addition'
    assert coll.exists('new',d3), 'Dataset 3 must exist after addition'

    coll.rem('new',d1)
    assert not coll.exists('new',d1), 'Dataset 1 does not exist after remove'
    assert coll.exists('new',d2), 'Dataset 2 must still exist after ds1 removal'
    assert coll.exists('new',d3), 'Dataset 3 must still exist after ds1 removal'

    coll.addlist('new', 'addition/removal test')    
    assert coll.length( 'new' ) == 0, "Adding a list should clear the previously found list"

def test_donkey2_add_to_unkown_list():

    from donkey.dataset import dataset_collection as collection
    from donkey.dataset import dataset
    
    ds = dataset()
    ds.name      = "dataset1"
    ds.runnumber = 12345
    ds.event     = "test_add"
    ds.created   = str(datetime.datetime.utcnow())

    co = collection( "test_donkey2_add_to_unknown_list", "new", "Newly created datasets" )

    with pytest.raises(KeyError):  # This will bomb
        co.add( 'old', ds )
    
def test_donkey2_multiadd_dataset_collection():

    from donkey.dataset import dataset_collection as collection
    from donkey.dataset import dataset
    
    ds = dataset()
    ds.name      = "dataset1"
    ds.runnumber = 12345
    ds.event     = "test_add"
    ds.created   = str(datetime.datetime.utcnow())

    co = collection( "test_donkey2_multiadd_dataset_collection", "new", "Newly created datasets" )

    co.add( 'new', ds )

    result = co.exists( 'new', ds )
    assert( result ), "The dataset can be found on the list"

    co.add( 'new', ds )
    assert co.length('new')==1 , "Adding multiple copies of a dataset should be ignored"
    
