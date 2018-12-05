#   Vic Chimenti
#   CPSC 5510 FQ18
#   p2 Distributed Hash Table
#   dht_node.py
#   created         11/26/2018
#   last modified   12/4/2018
#   Distributed Hash Table Node
#   /usr/local/python3/bin/python3


 # *********** TODO : Error checking : Try except
 #      IN node please check for 0 and deletes of values
 #      In node please return false when value not found



import sys                          # for system calls
import socket                       # for udp socket functionality
import pickle                       # for sending a list over socket
import argparse                     # for parsing command line arguments
import hashlib                      # SHA1 hash functionality
from collections import OrderedDict # for dictionary sorting


#***TODO make sure it will work even badly
#   convert to chord search
#   return value from dictionary not list


#   ***************     function definitions     ***************   #


# get the address and port for next node from hosfile contents
def getPath(c, v) :
    host_addr, host_port_str = c[v].split()
    host_port = int(host_port_str)

    return host_addr, host_port


# extract the request attributes
def getRequest(r) :
    a = str(r[0])
    p = int(r[1])
    h = int(r[2])
    op = str(r[3])
    k = str(r[4])
    v = str(r[5])

    return a, p, h, op, k, v


# extract the client address from the request
def getClient(r) :
    a = str(r[0])
    p = int(r[1])
    cli = (a, p)

    return cli


# calculate the node ID in hex
def getID(a, p) :
    p = socket.htonl(p)
    mh = hashlib.sha1()
    mh.update(repr(a).encode(charset))
    mh.update(repr(p).encode(charset))

    return mh.digest()


# calculate the node ID in hex
def hexID(a, p) :
    p = socket.htonl(p)
    mh = hashlib.sha1()
    mh.update(repr(a).encode(charset))
    mh.update(repr(p).encode(charset))

    return mh.hexdigest()


# calculate the node ID in hex
def getHash(k, v) :
    #v = socket.htonl(v)
    mh = hashlib.sha1()
    mh.update(repr(k).encode(charset))
    mh.update(repr(v).encode(charset))

    return mh.digest()


# calculate the node ID in hex
def getHashHex(k, v) :
    #v = socket.htonl(v)
    mh = hashlib.sha1()
    mh.update(repr(k).encode(charset))
    mh.update(repr(v).encode(charset))

    return mh.hexdigest()


# find current place in the ring
def getIndex(li, id) :
    i = li.index(id)

    return i


# get successors ID
def getSuccessor(li, i, c) :
    # make a shallow copy of the list
    temp = li.copy()
    # confirm that node is not the largest
    if i < (c - 1) :
        s = temp.pop((i + 1))
    # or else pop the first element
    else :
        s = temp.pop(0)

    # return the successor ID
    return s


# get predecessors ID
def getPredecessor(li, i, c) :
    # make a shallow copy of the list
    temp = li.copy()
    # confirm that node is not the largest
    if i > 0 :
        p = temp.pop((i - 1))
    # or else pop the first element
    else :
        p = temp.pop(c - 1)

    # return the predecessor ID
    return p


# get a full address from the value list with an index
def getAddr(vl, i) :
    # make a shallow copy of the of the list
    temp = vl.copy()
    # pop the element and get the value
    v = temp.pop(i)
    # split the contents on whitespace and scrub
    host_addr, host_port_str = v.split()
    # cast portno to int
    host_port = int(host_port_str)

    # return the address and port that matches the node
    return host_addr, host_port


# get a full address using the node_ID from the address dictionary
def getAddress(ID, d) :
    # get the value from the adress table
    a = d.get(ID)
    # split the contents on whitespace
    node_addr, node_port_str = a.split()
    # cast the port to an int
    node_port = int(node_port_str)

    # return the address and port that matches the node
    return node_addr, node_port


# return the full address of the node from the node id
def getNodeAddr(kl, vl, nd) :
    # get the index of the node from the sorted keyList
    i = kl.index(nd)
    # make a shallow copy of the value list
    temp = vl.copy()
    # pop element and get value
    v = temp.pop(i)
    # split the contents on whitespace and scrub
    host_addr, host_port_str = v.split()
    # cast portno to int
    host_port = int(host_port_str)

    # return the address and port that matches the node
    return host_addr, host_port


# def getValue(fingerList, my_index) :
#     try :
#         value = fingerList[my_index]
#     except OSError :
#         value = "ERROR : There value is empty"
#     finally :
#         return value


# put the value
# def putValue(fingerList, my_index, value) :
#     fingerList[my_index] = value


# def findNode(kl, key) :
#     i = 0
#     print ('i2 : ' + str(i))
#     while i < (count - 1) :
#         print ('i3 : ' + str(i))
#         if key > kl[i] :
#             print ('i4 : ' + str(i))
#             i += 1
#         else :
#             print ('i5 : ' + str(i))
#             break
#
#     return i


# # for the target key
# def findNode(start, key):
#     current=start
#     while distance(current.id, key) > \
#           distance(current.next.id, key):
#         current=current.next
#     return current


# # The largest possible node id is 2**k.
# def distance(a, b):
#     if a==b:
#         return 0
#     elif a<b:
#         return b-a;
#     else:
#         return (2**k)+(b-a);


# exclusive either-or bit comaparison to determine the ring distance
# def distance(node, key, d):
#     return (d[node] ^ key)

#TODO ***** not comparing KEY values, comparing key to value directly, need key
# the ring distance
def distance(a, b, c, li) :
    if (li[a] == b) :
        return 0
    elif (li[a] < b) :
        return (b - li[a])
    else :
        return ((2**c) + (b - li[a]))


# find the node from the full dictionary
def findNode(ID, key, successor, c, d) :
    # assign the current node
    node = ID
    # compare the search key to the current and successor IDs
    while distance (d[node], key, c, d) > distance (d[successor], key, c, d) :
        # assign the success to the node when the key is greater than current
        node = successor

    return node


# return the highest-key-value possible of the furthest node reachable
def findFurthest(i, s, c, l) :
    return ((2**c) + (s - l[i]))



def getNode(f, c, lk) :
    current = (c-1)
    next = (c-2)
    while lk[current] > f :
        current = next
        if next == 0 : break
        else : next -= 1

    idx = getIndex(lk, lk[current])
    return idx


# find the furtherest node available 2**m
def getFurthest(ID, my_ID, successor, c, lk) :
    furthest = findFurthest(ID, my_ID, successor, c, lk)

    return (getNode(furthest, c, lk))


#make a finger table from the full table
def makeFingers(idx, s_idx, lk, my_ID, successor_ID, c) :
    list2 = []
    list2.append(lk[idx])
    list2.append(lk[s_idx])
    list2.append(getFurthest(my_ID, successor_ID, c, lk))

    # get the new node's predecessor index via the id
    predecessor_ID = getPredecessor(lk, list2[2], c)
    list2.append(keyList.index(predecessor_ID))

    # sort the new list items
    list2.sort()

    return list2


# find the node index in a list
def findFinger(ID, key, successor_ID, li) :
    node = ID
    # compare the search key to the current and successor IDs
    while distance (li[node], key, c, li) > distance (li[successor], key, c, li) :
        # assign the success to the node when the key is greater than current
        node = successor

    return node


    # key, data = li[0].split()
    # if key >= k :
    #     return 1
    # else :
    #     return 0


# get the value when the node is not found yet
def getValue(ID, key, successor, c, d) :
    # find the correct node ID
    node = findNode(ID, key, successor, c, d)
    # get the value from the node pair
    try :
        value = d[node]
    except KeyError :
        # if the key is missing throw an error message
        value = 'ERROR: The Requested Search Key Does Not Exist'
    finally :
        # return whatever is in value:
            # either a true value
            # or an empty and-or whitespace/newline
            # or the error message
        return value


# or put the value when correct node ID is already found
def getValue(idx, li) :
    # find the correct node ID
    node = idx
    # get the value from the node pair
    try :
        v = li[node]
    except KeyError :
        # if the key is missing throw an error message
        v = 'ERROR: The Requested Search Key Does Not Exist'
    finally :
        # return whatever is in value:
            # either a true value
            # or an empty and-or whitespace/newline
            # or the error message
        return v


# put the value when the node is not found yet
def putValue(ID, key, successor, c, d, v) :
    # find the correct node ID
    node = findNode(ID, key, successor, c, d)
    # search for delete command
    if switch(v) == 1 :
        # if valid value, put value into dictionary
        d[node] = v
    else :
        # if delete parameter found then delete the key and return its value
        #value = d.pop(node)
        del d[node]


# or put the value when correct node ID is already found
def putValue (idx, li, v) :
    # assign the correct node ID
    node = idx
    # search for delete command
    if switch(v) == 1 :
        # if valid value, put value into dictionary
        li[node] = v
    else :
        # if delete parameter found then delete the key and return its value
        v = li.pop(node)
        #del d[node]



# determine if put contains a valid value or a delete parameter
def switch(v) :
    # if newline
    if v == CASE.get(a, 'default') :
        return 0
    # if whitespace
    elif v == CASE.get(b, 'default') :
        return 0
    # if empty string
    elif v == CASE.get(c, 'default') :
        return 0
    else :
        return 1





#   ***************     end function definitions     ***************   #




# DEFINE CONSTANTS
GET = 'get'
PUT = 'put'
INVALID = 'INVALID'
WHITESPACE = ' '
NEWLINE = '\n'
EMPTY = ''
CASE = {'a' : NEWLINE, 'b' : WHITESPACE, 'c' : EMPTY }

# define defaults
charset = "UTF-8"       # default encoding protocol
count = 0               # line counter for hostfile




# parse and assign command-line input
parser = argparse.ArgumentParser()
parser.add_argument('hostfile', type=str, nargs=1)
parser.add_argument('linenum', type=int, nargs=1)
args = parser.parse_args()

#  *** prints args as list elements *** #
print ('hostfile : ' + str(args.hostfile))
print ('linenum : ' + str(args.linenum))




# open file and assign to list
hostTable = {}
with open (args.hostfile[0], 'r') as file :
    content = file.readlines()
file.close()




# open the file and assign dictionary keys
file = open(args.hostfile[0], 'r')
for line in file.readlines() :
    host, port = line.split()
    port = int(port)
    k = getID(host, port)
    v = str(content[count])
    d = {k : v}
    hostTable.update(d)
    count += 1
file.close()


#TODO *********** consider only one list and one OrderedDict
# or no list and dict for address lookup and dict for storage

# make a sorted dictionary from the hostTable
addressTable = OrderedDict(sorted(hostTable.items()))
# make an iterable list of the sorted keys
keyList = list(addressTable.keys())
# make an iterable list of the sorted values
valueList = list(addressTable.values())
# make an ordered dictionary from the key list
fullTable = OrderedDict.fromkeys(keyList)  # change to hostTable
# make an ordered list from the key list
fullList = list(fullTable.keys())
# ts print of fingerTable
for ii in (addressTable) :
    print ("addressTable : " + str(ii))
for i in (keyList) :
    print ("keyList : " + str(i))
for k in (fullTable) :
    print ("fullTable: " + str(k))
for kk in (fullList) :
    print ("fullList: " + str(kk))
for jj in (hostTable) :
    print ("hostTable: " + str(jj))
for j in (valueList) :
    print ("valueList : " + str(j))




# split addr port info of my node
host_addr, host_port = getPath(content, args.linenum[0])
sc = host_addr, host_port
print ('host address and port from content : \n' + str(sc))





# calculate my current node hash value digest and hex
my_ID = getID(host_addr, host_port)
my_hex_ID = hexID(host_addr, host_port)
ha, hp = getNodeAddr(keyList, valueList, my_ID)
sa = ha, hp
print ('host address and port from fulllist : \n' + str(sa))



# find my place in the ring
my_index = keyList.index(my_ID)
print ("my_index : " + str(my_index))




# get successor information
successor_ID = getSuccessor(keyList, my_index, count)
successor_index = keyList.index(successor_ID)
successor_addr, successor_port = getAddr(valueList, successor_index)
print ("successor_ID : " + str(successor_ID))
print ("successor_index : " + str(successor_index))
print ("successor_addr : " + successor_addr)
print ("successor_port : " + str(successor_port))




# get predecessor information
predecessor_ID = getPredecessor(keyList, my_index, count)
predecessor_index = keyList.index(predecessor_ID)
predecessor_addr, predecessor_port = getAddr(valueList, predecessor_index)
print ("predecessor_ID : " + str(predecessor_ID))
print ("predecessor_index : " + str(predecessor_index))
print ("predecessor_addr : " + predecessor_addr)
print ("predecessor_port : " + str(predecessor_port))




# create a udp socket
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((host_addr, host_port))
print ("Listening on Address, Port : " + str((host_addr, host_port)))




# listen for communication
while True :
    message, address = sock.recvfrom(4096)
    request = pickle.loads(message)
    print ('\nreceived {} bytes from {}'.format(len(message), address))
    print ('request received : ' + str(request))


# ****************** ENSURE GET OPERATION WITH NO VALUE IS VALID *********** #

    # if a valid message arrived
    if message :
        # assign request components to local varariables
        cli_addr, cli_port, hops, operation, key, value = getRequest(request)
        print ("cli_addr : " + str(cli_addr))
        # get hash and hex value of user key value pair
        client_hex_key = getHashHex(key, value)
        print ("client_hex_key : " + str(client_hex_key))
        client_key = getHash(key, value)
        print ("client_key : " + str(client_key))

        # get the node_ID
        #node_ID = findNode(my_ID, client_key, successor_ID, count, fullTable) #(keyList, client_key)

        # make fingerTable as a list of two nodes
        fingerTable = makeFingers(my_index, successor_index, keyList, my_ID, successor_ID, count)
        num = findFinger(my_index, client_key, successor_index, fingerTable)
        #print ("node_index : " + str(node_ID))
        # increment each hop
        hops += 1


# **** This logic may have to change with chord ****** #

        # if the value matches current node return directly to the client
        if num == 0 : #node_ID == my_ID :

            # determine operation
            if operation.lower() == GET :
                value = getValue(node_ID, fullTable)#(fingerList, my_index)

            # or else put the value
            elif operation.lower() == PUT :
                # put the value
                putValue(node_ID, fullTable, value)#(fingerList, my_index, value)

            # or else the operation is invalid so prepare error message for client
            else :
                value = "ERROR Invalid Operation Requested : OP : " + operation
                print ('ERROR : ' + key)

            # gather client address for response
            next_addr = getClient(request)

            # return to client cli-hex, node-hex, hops, key_str, value_str
            response = client_hex_key, my_hex_ID, hops, key, str(value)

        # or else get the address of the successor node
        elif num == 1 : #node_ID == successor_ID :

            # set the next address for outgoing response
            next_addr = successor_addr, successor_port
            # forward to next node hash-key, hash-node, hops, key_str, value_str
            response = cli_addr, cli_port, hops, operation, key, value

        # or else get the address of the next node
        else :
            print ('oops : ')
        #
        #     # call for the address of the correct node ID
        #     n_addr, n_port = getAddress(node_ID, addressTable)#(valueList, node_index)
        #     # set the next address for outgoing response
        #     next_addr = n_addr, n_port
        #     # forward to next node hash-key, hash-node, hops, key_str, value_str
        #     response = cli_addr, cli_port, hops, operation, key, value




        # pickle the valid response
        message = pickle.dumps(response)




    # or else there was no valid message received
    else :
        # generate error message
        response = "ERROR : Invalid Message: Please Resend... "
        # attempt to retreive receive from address for error reply
        next_addr = address
        # pickle the error message
        message = pickle.dumps(response)




    # send the message
    bytes_sent = sock.sendto(message, next_addr)
    print ('\nsent {} bytes to {}'.format(bytes_sent, next_addr))
    print ('response sent : ' + str(response))




# close socket and exit program
sock.close
sys.exit()





















#  ***************** Functions for updating tables after joins/leaves ******** #
# update finger table
# def update(node):
#     for x in range(k):
#         oldEntry=node.finger[x]
#         node.finger[x]=findNode(oldEntry,
#                           (node.id+(2**x)) % (2**k))
#
# # find the correct finger
# def findFinger(node, key):
#     current=node
#     for x in range(k):
#         if distance(current.id, key) > \
#            distance(node.finger[x].id, key):
#             current=node.finger[x]
#     return current
#
# # look up the correct finger
# def lookup(start, key):
#     current=findFinger(start, key)
#     next=findFinger(current, key)
#     while distance(current.id, key) > \
#           distance(next.id, key):
#         current=next
#         next=findFinger(current, key)
#     return current

#   eof
