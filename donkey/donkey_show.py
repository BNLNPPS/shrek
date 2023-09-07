import tabulate

from donkey.dataset import dataset_collection as collection
from donkey.dataset import dataset

coll = collection( 'donkey-listener' )
#coll = collection( 'test_donkey2_add_remove_dataset_collection')

containers = list(coll.db.getall())

meta = [ x for x in containers if '.meta' in x]
cons = [ x for x in containers if '.meta' not in x]

for con in cons:
    dataset = coll.getall( con )
    print(con)
    print("---------------------------------------------")
    if len(dataset):
        header = dataset[0].keys()
        rows   = [ x.values() for x in dataset ]
        print(tabulate.tabulate(rows, header, tablefmt='grid'))


for con in cons:
    dataset = coll.getall( con )
    print(con)
    print("---------------------------------------------")
    if len(dataset):
        header = dataset[0].keys()
        rows   = [ x.values() for x in dataset ]
        print(tabulate.tabulate(rows, header, tablefmt='grid'))
