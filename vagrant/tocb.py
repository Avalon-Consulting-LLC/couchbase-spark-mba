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

'''
select
    least(itemid, secitemid) as item1,
    greatest(itemid, secitemid) as item2,
    count(*) as cnt
from
    retail
        unnest items as itemid
        unnest retail.items as secitemid
where
    itemid != secitemid
group by
    least(itemid, secitemid),
    greatest(itemid, secitemid)
order by
    cnt desc
limit
    10
;
'''
