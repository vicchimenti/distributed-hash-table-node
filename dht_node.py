#   Vic Chimenti
#   CPSC 5510 FQ18
#   p2 Distributed Hash Table
#   dht_node.py
#   created         11/26/2018
#   last modified   11/27/2018


#   Distributed Hash Table Node


# define the size of the table
def distance(a, b):
    if a==b:
        return 0
    elif a<b:
        return b-a;
    else:
        return (2**k)+(b-a);

# find the node
def findNode(start, key):
    current=start
    while distance(current.id, key) > \
          distance(current.next.id, key):
        current=current.next
    return current

# get the value
def lookup(start, key):
    node=findNode(start, key)
    return node.data[key]

# put the value
def store(start, key, value):
    node=findNode(start, key)
    node.data[key]=value

# update finger table
def update(node):
    for x in range(k):
        oldEntry=node.finger[x]
        node.finger[x]=findNode(oldEntry,
                          (node.id+(2**x)) % (2**k))

# 
def findFinger(node, key):
    current=node
    for x in range(k):
        if distance(current.id, key) > \
           distance(node.finger[x].id, key):
            current=node.finger[x]
    return current

def lookup(start, key):
    current=findFinger(start, key)
    next=findFinger(current, key)
    while distance(current.id, key) > \
          distance(next.id, key):
        current=next
        next=findFinger(current, key)
    return current

#   eof
