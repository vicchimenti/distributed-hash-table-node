#   Vic Chimenti
#   CPSC 5510 FQ18
#   p2 Distributed Hash Table
#   dht_node.py
#   created         11/26/2018
#   last modified   12/5/2018
#   Distributed Hash Table Node
#   /usr/local/python3/bin/python3




 # *********** TODO : Error checking : Try except
 #      IN node please check for 0 and deletes of values
 #      In node please return false when value not found
 #      check that successor index does not go out of bounds and instead wraps to zero
 #      ensure parallel list sytem stores and retreives values correctly
 # Once Done :
 #      Implement O(logn) solutions





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




#make a finger table from the full table
def makeFingers(idx, s_idx) :
    list2 = []
    list2.append(idx)
    list2.append(s_idx)
    return list2




# find the node index in a list of two
def findFinger(key, li) :
    if key < li[1] :
        return 0
    else :
        return 1




# or put the value when correct node ID is already found
def getValue(idx, li) :
    # get the value from the node pair
    try :
        v = li[idx]
    except KeyError :
        # if the key is missing throw an error message
        v = 'ERROR: The Requested Search Key Does Not Exist'
    finally :
        # return whatever is in value:
            # either a true value
            # or an empty and-or whitespace/newline
            # or the error message
        return v




# or put the value when correct node ID is already found
def putValue (idx, v, lk, lv) :
    # assign the correct node ID
    node = idx
    # search for delete command
    if switch(v) == 1 :
        # if valid value, put value into dictionary
        lv[node] = v
    else :
        # if delete parameter found then delete the key and return its value
        v = lv.pop(node)
        ignore = lk.pop(node)
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




# launch argparse
try :
    parser = argparse.ArgumentParser()
except SystemExit :
    print ('ERROR: Invalid Command Line Input: Please Re-run the Program')
    exc = sys.exc_info()[1]
    print (exc)
    sys.exit ('Exiting Program')

# parse first command line argument
try :
    parser.add_argument('hostfile', type=str, nargs=1)
except IndexError :
    error_message = "ERROR No Valid Command Line Input"
    print (error_message)
    sys.exit ("Exiting Program")
except KeyError :
    error_message = "ERROR Invalid Command Line Entry"
    print (error_message)
    sys.exit ("Exiting Program")

# parse second command line argument
try :
    parser.add_argument('linenum', type=int, nargs=1)
except IndexError :
    error_message = "ERROR No Valid Command Line Input"
    print (error_message)
    sys.exit ("Exiting Program")
except KeyError :
    error_message = "ERROR Invalid Command Line Entry"
    print (error_message)
    sys.exit ("Exiting Program")

# declare argparse type variable
try :
    args = parser.parse_args()
except SystemExit :
    print ('ERROR: Invalid Command Line Input: Please Re-run the Program')
    exc = sys.exc_info()[1]
    print (exc)
    sys.exit ('Exiting Program')

#  *** prints args as list elements *** #
print ('hostfile : ' + str(args.hostfile))
print ('linenum : ' + str(args.linenum))




# open file and assign to list
hostTable = {}
try :
    with open (args.hostfile[0], 'r') as file :
        content = file.readlines()
except EOFError :
    error_message = "ERROR: No data in file"
    print (error_message)
    file.close()
    sys.exit ("Exiting Program")
file.close()




# open the file and assign dictionary keys
try :
    file = open(args.hostfile[0], 'r')
except AttributeError :
    error_message = "ERROR: Assignment from file failed"
    print (error_message)
    file.close()
    sys.exit ("Exiting Program")

#  assign dictionary keys
try :
    for line in file.readlines() :
        host, port = line.split()
        port = int(port)
        k = getID(host, port)
        v = str(content[count])
        d = {k : v}
        hostTable.update(d)
        count += 1
except IndexError :
    error_message = "ERROR: Assignment from file failed : "
    print (error_message)
    exc = sys.exc_info()[1]
    print (exc)
    file.close()
    sys.exit ("Exiting Program")
file.close()



# *******  Make lists and dictionaries from the original dictionary   ******** #

# make a sorted dictionary from the hostTable
try :
    addressTable = OrderedDict(sorted(hostTable.items()))
except KeyError :
    error_message = "ERROR: Ordered Dictionary Assignment Failed : "
    print (error_message)
    exc = sys.exc_info()[1]
    print (exc)
    sys.exit ("Exiting Program")

# make an iterable list of the sorted keys
try :
    keyList = list(addressTable.keys())
except AttributeError :
    error_message = "ERROR: KeyList from Ordered Dictionary Assignment Failed : "
    print (error_message)
    exc = sys.exc_info()[1]
    print (exc)
    sys.exit ("Exiting Program")

# make an iterable list of the sorted addresses
try :
    addressList = list(addressTable.values())
except AttributeError :
    error_message = "ERROR: addressList from Ordered Dictionary Assignment Failed : "
    print (error_message)
    exc = sys.exc_info()[1]
    print (exc)
    sys.exit ("Exiting Program")

# make an ordered dictionary from the key list
try :
    fullTable = OrderedDict.fromkeys(keyList)  # change to hostTable
except KeyError :
    error_message = "ERROR: Ordered Dictionary from KeyList Assignment Failed : "
    print (error_message)
    exc = sys.exc_info()[1]
    print (exc)
    sys.exit ("Exiting Program")

# make an ordered list from the key list
try :
    fullList = list(fullTable.keys())
except AttributeError :
    error_message = "ERROR: fullList from Ordered Dictionary Assignment Failed : "
    print (error_message)
    exc = sys.exc_info()[1]
    print (exc)
    sys.exit ("Exiting Program")

# make an ordered list of empty values from the key list
try :
    valueList = list(fullTable.values())
except AttributeError :
    error_message = "ERROR: valueList from Ordered Dictionary Assignment Failed : "
    print (error_message)
    exc = sys.exc_info()[1]
    print (exc)
    sys.exit ("Exiting Program")




# ts print of dictionarys and lists compiled from the hostfile
for ii in (addressTable) :
    print ("addressTable : " + str(ii))
for i in (keyList) :
    print ("keyList : " + str(i))
for k in (fullTable) :
    print ("fullTable: " + str(k))
for kk in (fullList) :
    print ("fullList: " + str(kk))
for jjj in (valueList) :
    print ("valueList : " + str(jjj))
for jj in (hostTable) :
    print ("hostTable: " + str(jj))
for j in (addressList) :
    print ("addressList : " + str(j))

sys.exit()


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

        # make fingerTable as a list of two nodes
        fingerTable = makeFingers(my_ID, successor_ID)
        idx = findFinger(client_key, fingerTable)

        # increment each hop
        hops += 1




        # if the value matches current node return directly to the client
        if idx == 0 :

            # determine operation
            if operation.lower() == GET :
                # get the value
                value = getValue(idx, valueList)

            # or else put the value
            elif operation.lower() == PUT :
                # put the value
                putValue(idx, value, valueList, keyList)

            # or else the operation is invalid so prepare error message for client
            else :
                value = "ERROR Invalid Operation Requested : OP : " + operation
                print ('ERROR : ' + key)

            # gather client address for response
            next_addr = getClient(request)

            # return to client cli-hex, node-hex, hops, key_str, value_str
            response = client_hex_key, my_hex_ID, hops, key, str(value)

        # or else get the address of the successor node
        else :

            # set the next address for outgoing response
            next_addr = successor_addr, successor_port
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

#   eof
