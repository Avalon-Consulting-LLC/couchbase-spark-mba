from couchbase.bucket import Bucket

bucket = Bucket("couchbase://localhost/retail")
f = open('retail.txt','r')
count = 1
inserts = dict()

for line in f:
    items = map(int, line.strip().split(" "))
    inserts["order::%d" % count] = {'type': 'order', 'items': items}
    count += 1

bucket.insert_multi(inserts)
