#   Vic Chimenti
#   CPSC 5510 FQ18
#   p2 Distributed Hash Table
#   dht_node.py
#   created         11/26/2018
#   last modified   12/3/2018
#   Distributed Hash Table Node
#   /usr/local/python3/bin/python3




import sys                          # for system calls
import socket                       # for udp socket functionality
import pickle                       # for sending a list over socket
import argparse                     # for parsing command line arguments
import hashlib                      # SHA1 hash functionality
from collections import OrderedDict # for dictionary sorting


#***TODO make sure it will work even badly
#   review dictionary, list, map data types


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

# **************** TRACE THIS TO SEE WHO USES IT ******************** #
# calculate the node ID in hex
def getHash(k, v) :
    #v = socket.htonl(v)
    mh = hashlib.sha1()
    mh.update(repr(k).encode(charset))
    mh.update(repr(v).encode(charset))

    return mh.digest()

# **************** TRACE THIS TO SEE WHO USES IT ******************** #
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


def getValue(fingerList, my_index) :
    try :
        value = fingerList[my_index]
    except OSError :
        value = "ERROR : There value is empty"
    finally :
        return value

    # # try :
    # #     ignore, value = value
    # # #attribute error
    # # except OSError :
    # #     value = "ERROR : There value is empty"
    #
    # return value


# put the value
def putValue(fingerList, my_index, value) :
    fingerList[my_index] = value


def findNode(kl, key) :
    i = 0
    print ('i2 : ' + str(i))
    while i < (count - 1) :
        print ('i3 : ' + str(i))
        if key > kl[i] :
            print ('i4 : ' + str(i))
            i += 1
        else :
            print ('i5 : ' + str(i))
            break
            
    return i





# # define the size of the ring
# def distance(a, b):
#     return a^b
# # This is a clockwise ring distance function.
# It depends on a globally defined k, the key size.
# The largest possible node id is 2**k.
# def distance(a, b):
#     if a==b:
#         return 0
#     elif a<b:
#         return b-a;
#     else:
#         return (2**k)+(b-a);
#
# # find the node
# def findNode(start, key):
#     current=start
#     while distance(current.id, key) > \
#           distance(current.next.id, key):
#         current=current.next
#     return current
#
# # get the value
# def getValue(start, key):
#     node=findNode(start, key)
#     return node.data[key]
#
# # put the value
# def putValue(start, key, value):
#     node=findNode(start, key)
#     node.data[key]=value


#   ***************     end function definitions     ***************   #




# DEFINE CONSTANTS
GET = 'get'
PUT = 'put'
WHITESPACE = ' '
INVALID = 'INVALID'
NEWLINE = '\n'

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
fingerTable = OrderedDict.fromkeys(keyList)
# make an ordered list from the key list
fingerList = list(fingerTable.keys())
# ts print of fingerTable
for ii in (addressTable) :
    print ("addressTable : " + str(ii))
for i in (keyList) :
    print ("keyList : " + str(i))
for k in (fingerTable) :
    print ("fingerTable: " + str(k))
for kk in (fingerList) :
    print ("fingerList: " + str(kk))
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
print ('host address and port from fingerlist : \n' + str(sa))



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




# TODO :
#   search for client_key using chord, not exact match




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
        # find the node's place in the ring
        node_index = findNode(keyList, client_key)
        print ("node_index : " + str(node_index))
        # increment each hop
        hops += 1




        # if the value matches current node return directly to the client
        if node_index == my_index :

            # determine operation
            if operation.lower() == GET :
                value = getValue(fingerList, my_index)

            # or else put the value
            elif operation.lower() == PUT :
                # put the value
                putValue(fingerList, my_index, value)

            # or else the operation is invalid so prepare error message for client
            else :
                value = "ERROR Invalid Operation Requested : OP : " + operation
                print ('ERROR : ' + key)

            # gather client address for response
            next_addr = getClient(request)

            # return to client cli-hex, node-hex, hops, key_str, value_str
            response = client_hex_key, my_hex_ID, hops, key, str(value)

        # or else get the address of the successor node
        elif node_index == successor_index :
            next_addr = successor_addr, successor_port
            # forward to next node hash-key, hash-node, hops, key_str, value_str
            response = cli_addr, cli_port, hops, operation, key, value

        # or else get the address of the next node
        else :
            n_addr, n_port = getAddr(valueList, node_index)
            next_addr = n_addr, n_port
            # forward to next node hash-key, hash-node, hops, key_str, value_str
            response = cli_addr, cli_port, hops, operation, key, value




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
