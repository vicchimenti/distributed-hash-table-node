#   Vic Chimenti
#   CPSC 5510 FQ18
#   p2 Distributed Hash Table
#   dht_node.py
#   created         11/26/2018
#   last modified   12/1/2018
#   Distributed Hash Table Node
#   /usr/local/python3/bin/python3




import sys                          # for system calls
import socket                       # for udp socket functionality
import pickle                       # for sending a list over socket
import argparse                     # for parsing command line arguments
import hashlib                      # SHA1 hash functionality
from collections import OrderedDict # for dictionary sorting




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
    v = int(r[5])

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
    v = socket.htonl(v)
    mh = hashlib.sha1()
    mh.update(repr(k).encode(charset))
    mh.update(repr(v).encode(charset))

    return mh.digest()


# calculate the node ID in hex
def getHashHex(k, v) :
    v = socket.htonl(v)
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


# return the full address of the node
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







# define the size of the ring
def distance(a, b):
    return a^b

# find the node
def findNode(start, key):
    current=start
    while distance(current.id, key) > \
          distance(current.next.id, key):
        current=current.next
    return current

# get the value
def getValue(start, key):
    node=findNode(start, key)
    return node.data[key]

# put the value
def putValue(start, key, value):
    node=findNode(start, key)
    node.data[key]=value


#   ***************     end function definitions     ***************   #




# DEFINE CONSTANTS
GET = 'get'
PUT = 'put'
WHITESPACE = ' '
INVALID = 'INVALID'

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

# make a sorted dictionary from the hostTable
fingerTable = OrderedDict(sorted(hostTable.items()))
# make an iterable list of the sorted keys
keyList = list(fingerTable.keys())
# make an iterable list of the sorted values
valueList = list(fingerTable.values())
# ts print of fingerTable
for ii in (fingerTable) :
    print ("fingerTable : " + str(ii))
for i in (keyList) :
    print ("keyList : " + str(i))
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




# get successors ID
successor_ID = getSuccessor(valueList, my_Index, count)
successor_index = (my_Index + 1)
print ("successor_ID : " + str(successor_ID))
print ("successor_index : " + str(successor_index))




# TODO :
#   add hash functionality to the client sent key
#   update logic for routing response
#   create new variable for the hash cli key
#   redo next_addr if sending to a new node if not successor




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




    # if a valid message arrived
    if message :
        # assign request components to local varariables
        cli_addr, cli_port, hops, operation, key, value = getRequest(request)
        # get hash and hex value of user key value pair
        client_hex_key = getHashHex(key, value)
        client_key = getHash(key, value)
        # find the node's place in the ring
        node_index = keyList.index(client_key)
        print ("node_index : " + str(node_index))
        # increment each hop
        hops += 1




        # # determine operation
        # if operation.lower() == GET :
        #     node_index = getIndex(keyList, client_key)
        # # or else put the value
        # elif operation.lower() == PUT :
        #     node_index = getIndex(keyList, client_key, value)
        # # or else the operation is invalid so prepare error message for client
        # else :
        #     node_index = INVALID
        #     key = "ERROR Invalid Operation Requested : OP : " + operation
        #     print ('ERROR : ' + key)




        # if the value matches current node return directly to the client
        if node_index == my_index :
            # gather client address for response
            next_addr = getClient(request)
            # return to client hash-key-hex, hash-node, hops, key_str, value_str-or-error_msg
            response = client_hex_key, my_hex_ID, hops, key, str(value)

        # or else get the address of the successor node
        elif node_index == successor_index :
            s_addr, s_port = getNodeAddr(keyList, valueList, node)
            next_addr = s_addr, s_port
            # forward to next node hash-key, hash-node, hops, key_str, value_str
            response = cli_addr, cli_port, hops, operation, key, value

        # # or else the client requested invalid operation
        # elif node == INVALID :
        #     # gather client address for error response
        #     next_addr = getClient(request)
        #     # return to client hash-key-hex, hash-node, hops, key_str, value_str-or-error_msg
        #     response = client_hex_key, my_hex_ID, hops, key, str(value)

        # or else get the address of the next node
        else :
            n_addr, n_port = getNodeAddr(addressList, fingerList, node)
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
