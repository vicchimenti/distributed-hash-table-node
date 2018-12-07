#   Vic Chimenti
#   CPSC 5510 FQ18
#   p2 Distributed Hash Table
#   dht_node.py
#   created         11/26/2018
#   last modified   12/6/2018
#   Distributed Hash Table Node
#   /usr/local/python3/bin/python3




 # *********** TODO : Error checking : Try except
 #      IN node please check for 0 and deletes of values
 #      In node please return false when value not found
 #      check that successor index does not go out of bounds and instead wraps to zero
 #      ensure parallel list sytem stores and retreives values correctly
 # Once Done :
 #      Implement O(logn) solutions
 # ready to try





import sys                          # for system calls
import socket                       # for udp socket functionality
import pickle                       # for sending a list over socket
import argparse                     # for parsing command line arguments
import hashlib                      # SHA1 hash functionality
from collections import OrderedDict # for dictionary sorting
from collections import Mapping




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
def getHash(k) :
    #v = socket.htonl(v)
    mh = hashlib.sha1()
    mh.update(repr(k).encode(charset))
    return mh.digest()




# calculate the node ID in hex
def getHashHex(k) :
    #v = socket.htonl(v)
    mh = hashlib.sha1()
    mh.update(repr(k).encode(charset))
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
    # confirm that node is not the smallest
    if i > 0 :
        p = temp.pop((i - 1))
    # or else pop the last element
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




#make a finger table from the full table
def makeFingers(p_id, id, s_id) :
    list2 = []
    # the predecessor node
    list2.append(p_id)
    # my node
    list2.append(id)
    # the successor node
    list2.append(s_id)
    return list2




# find the node index in a list of two
def findFinger(key, idx, li) :
    # when my node is the first node compare to the largest node (predecessor)
    if idx == 0 :
        # when the key is greater than the largest node
        if key > li[0] :
            # store the value now in node 0
            return 0
        else :
            # call the successor
            return 1
    # or else my node is not the first node
    else :
        # compare to my node
        if key > li[1] :
            # call the successor
            return 1
        else :
            # store the value now
            return 0




# or put the value when correct node ID is already found
def getValue(k) :
    # get the value from the node pair
    try :
        v = crud.get(k)
    except KeyError :
        # if the key is missing throw an error message
        v = 'ERROR: The Requested Search Key Does Not Exist'
        exc = sys.exc_info()[1]
        print (v + '\n' + exc)
    finally :
        # return whatever is in value:
            # either a true value
            # or an empty and-or whitespace/newline
            # or the error message
        return v




# or put the value when correct node ID is already found
def putValue (k, v) :
    # assign the key value pair to a map item
    #d = {k : v}
    map(key, value)[(k, v)]
    # ensure key does not already exist
    if k not in crud.keys() :
        # ensure no delete command
        if switch(v) == 1 :
            # if valid value, put new value into dictionary
            crud.update(map(key, value))
            for k, v in crud.items() :
                print(k, v)
        else :
            # if delete parameter found then delete the key and return its value
            try :
                confirm_deleted = crud.pop(map(key, value))
            except KeyError :
                print ('value deleted : ' + confirm_deleted)
                exc = sys.exc_info()[1]
                print (exc)
    # or else the key already exists
    else :
        # ensure no delete command
        if switch(v) == 1 :
            # if valid value, put new value into dictionary
            del crud[k]
            crud.update(map(key, value))
        else :
            # if delete parameter found then delete the key and return its value
            try :
                confirm_deleted = crud.pop(map(key, value))
            except KeyError :
                print ('value deleted : ' + confirm_deleted)
                exc = sys.exc_info()[1]
                print (exc)






# *** TODO : Switch Case ERROR ******************

# determine if put contains a valid value or a delete parameter
def switch(v) :
    # if newline
    if v == NEWLINE :
        return 0
    # if whitespace
    elif v == WHITESPACE :
        return 0
    # if empty string
    elif v == EMPTY :
        return 0
    # or else there is no delete request
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

# define defaults
charset = "UTF-8"       # default encoding protocol
count = 0               # line counter for hostfile
crud = {}               # data dictionary to store requested key:value pairs




# launch argparse
try :
    parser = argparse.ArgumentParser()
except SystemExit :
    error_message = "ERROR: Invalid Command Line Input : Re-run the Program"
    print (error_message)
    exc = sys.exc_info()[1]
    print (exc)
    sys.exit ('Exiting Program')

# parse first command line argument
try :
    parser.add_argument('hostfile', type=str, nargs=1)
except IndexError :
    error_message = "ERROR No Valid Command Line Input"
    print (error_message)
    exc = sys.exc_info()[1]
    print (exc)
    sys.exit ("Exiting Program")
except KeyError :
    error_message = "ERROR Invalid Command Line Entry"
    print (error_message)
    exc = sys.exc_info()[1]
    print (exc)
    sys.exit ("Exiting Program")

# parse second command line argument
try :
    parser.add_argument('linenum', type=int, nargs=1)
except IndexError :
    error_message = "ERROR No Valid Command Line Input"
    print (error_message)
    exc = sys.exc_info()[1]
    print (exc)
    sys.exit ("Exiting Program")
except KeyError :
    error_message = "ERROR Invalid Command Line Entry"
    print (error_message)
    exc = sys.exc_info()[1]
    print (exc)
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
    exc = sys.exc_info()[1]
    print (exc)
    file.close()
    sys.exit ("Exiting Program")
file.close()




# open the file to assign dictionary keys
try :
    file = open(args.hostfile[0], 'r')
except AttributeError :
    error_message = "ERROR: Assignment of file variable failed"
    print (error_message)
    exc = sys.exc_info()[1]
    print (exc)
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




# split addr port info of my node
try :
    host_addr, host_port = getPath(content, args.linenum[0])
except AttributeError :
    error_message = "ERROR: Path Assignment from file contents failed : "
    print (error_message)
    exc = sys.exc_info()[1]
    print (exc)
    sys.exit ("Exiting Program")

# assign to tuple
sc = host_addr, host_port
print ('host address and port from file contents : ' + str(sc))





# calculate my current node hash value digest
try :
    my_ID = getID(host_addr, host_port)
except Exception :
    error_message = "ERROR: Host ID Hash digest failed : "
    print (error_message)
    exc = sys.exc_info()[1]
    print (exc)
    sys.exit ("Exiting Program")

# calculate my current node hex value digest
try :
    my_hex_ID = hexID(host_addr, host_port)
except Exception :
    error_message = "ERROR: Host ID Hash Hex failed : "
    print (error_message)
    exc = sys.exc_info()[1]
    print (exc)
    sys.exit ("Exiting Program")




# find my place in the ring
try :
    my_index = keyList.index(my_ID)
except AttributeError :
    error_message = "ERROR: Assigning Index from list failed : "
    print (error_message)
    exc = sys.exc_info()[1]
    print (exc)
    sys.exit ("Exiting Program")
print ("my_index : " + str(my_index))




# get successor ID
try :
    successor_ID = getSuccessor(keyList, my_index, count)
except Exception :
    error_message = "ERROR: Successor ID Hash Hex failed : "
    print (error_message)
    exc = sys.exc_info()[1]
    print (exc)
    sys.exit ("Exiting Program")

# get successor index
try :
    successor_index = keyList.index(successor_ID)
except AttributeError :
    error_message = "ERROR: Assigning Successor Index from list failed : "
    print (error_message)
    exc = sys.exc_info()[1]
    print (exc)
    sys.exit ("Exiting Program")

# get successor address information
try :
    successor_addr, successor_port = getAddr(addressList, successor_index)
except AttributeError :
    error_message = "ERROR: Successor Path Assignment from list failed : "
    print (error_message)
    exc = sys.exc_info()[1]
    print (exc)
    sys.exit ("Exiting Program")

# print successor information
print ("successor_ID : " + str(successor_ID))
print ("successor_index : " + str(successor_index))
print ("successor_addr : " + successor_addr)
print ("successor_port : " + str(successor_port))




# get predecessor information for *** TroubleShooting Purposes Only ********* #
predecessor_ID = getPredecessor(keyList, my_index, count)
predecessor_index = keyList.index(predecessor_ID)
predecessor_addr, predecessor_port = getAddr(addressList, predecessor_index)
print ("predecessor_ID : " + str(predecessor_ID))
print ("predecessor_index : " + str(predecessor_index))
print ("predecessor_addr : " + predecessor_addr)
print ("predecessor_port : " + str(predecessor_port))




# create a udp socket
try :
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
except ConnectionError :
    error_message = "ERROR Establishing a Socket"
    print (error_message)
    exc = sys.exc_info()[1]
    print (exc)
    sys.exit ("Exiting Program")

# bind the socket
try :
    sock.bind((host_addr, host_port))
except ConnectionError :
    error_message = "ERROR ConnectionError Binding the Host and Port"
    print (error_message)
    exc = sys.exc_info()[1]
    print (exc)
    sys.exit ("Exiting Program")
except OSError :
    error_message = "ERROR Port Already in Use"
    print (error_message)
    exc = sys.exc_info()[1]
    print (exc)
    sys.exit ("Exiting Program")
print ("Listening on Address, Port : " + str((host_addr, host_port)))




# listen for communication
while True :

    # receive message
    try :
        message, address = sock.recvfrom(4096)
    except ConnectionError :
        error_message = "ERROR Receiving Client Message"
        status = "Invalid Message"
        print (error_message)
        exc = sys.exc_info()[1]
        print (exc)

    # unpickle the request
    try :
        request = pickle.loads(message)
    except pickle.UnpicklingError :
        error_message = "ERROR: UnPickling the Message : "
        print (error_message)
        exc = sys.exc_info()[1]
        print (exc)
    finally :
        # display the connection details
        print ('\nreceived {} bytes from {}'.format(len(message), address))
        print ('request received : ' + str(request))


# ************TODO : ENSURE GET OPERATION WITH NO VALUE IS VALID *********** #

    # if a valid message arrived
    if message :
        # assign request components to local variables
        try :
            cli_addr, cli_port, hops, operation, key, value = getRequest(request)
        except Exception :
            error_message = "ERROR: Parsing the Client Request : "
            print (error_message)
            exc = sys.exc_info()[1]
            print (exc)
        finally :
            # display the client address
            print ("cli_addr : " + str(cli_addr))

        # get hash and hex value of user key value pair
        try :
            client_hex_key = getHashHex(key)
        except Exception :
            error_message = "ERROR: Client ID Hash Hex failed : "
            print (error_message)
            exc = sys.exc_info()[1]
            print (exc)
        finally :
            # display the client hash hex
            print ("client_hex_key : " + str(client_hex_key))

        # get the client key hash
        try :
            client_key = getHash(key)
        except Exception :
            error_message = "ERROR: Client Key Hash failed : "
            print (error_message)
            exc = sys.exc_info()[1]
            print (exc)
        finally :
            # display the client key hash
            print ("client_key : " + str(client_key))

        # make fingerTable as a list of three nodes
        try :
            fingerTable = makeFingers(predecessor_ID, my_ID, successor_ID)
        except Exception :
            error_message = "ERROR: FingerTable Assignment failed : "
            print (error_message)
            exc = sys.exc_info()[1]
            print (exc)

        # get the finger list index
        try :
            idx = findFinger(client_key, my_index, fingerTable)
        except Exception :
            error_message = "ERROR: Finger Index Assignment failed : "
            print (error_message)
            exc = sys.exc_info()[1]
            print (exc)


        # increment each hop
        hops += 1




        # if the value matches current node return directly to the client
        if idx == 0 :

            # determine operation
            if operation.lower() == GET :
                # get the value
                try :
                    value = getValue(client_key)
                except Exception :
                    error_message = "ERROR: Value Assignment from CRUD failed : "
                    print (error_message)
                    exc = sys.exc_info()[1]
                    print (exc)



#  ***** TODO : ERROR PUT name 'a' is not defined in switch case
            # or else put the value
            elif operation.lower() == PUT :
                # put the value
                try :
                    putValue(client_key, value)
                except Exception :
                    error_message = "ERROR: Put Value on CRUD failed : "
                    print (error_message)
                    exc = sys.exc_info()[1]
                    print (exc)

            # or else the operation is invalid so prepare error message for client
            else :
                value = "ERROR Invalid Operation Requested : OP : " + operation
                print ('ERROR : ' + key)



# ********** TODO : Ensrure that error value prints when necessary to client


            # gather client address for response
            try :
                next_addr = getClient(request)
            except Exception :
                error_message = "ERROR: Client Return Address failed : "
                print (error_message)
                exc = sys.exc_info()[1]
                print (exc)
            finally :
                # return to client cli-hex, node-hex, hops, key_str, value_str
                response = client_hex_key, my_hex_ID, hops, key, str(value)

        # or else get the address of the successor node
        else :

            # set the next address for outgoing response
            next_addr = successor_addr, successor_port
            # forward to next node hash-key, hash-node, hops, key_str, value_str
            response = cli_addr, cli_port, hops, operation, key, value




        # pickle the valid response
        try :
            message = pickle.dumps(response)
        except pickle.PickleError :
            error_message = "ERROR: Pickling the Message : "
            print (error_message)
            exc = sys.exc_info()[1]
            print (exc)



    # or else there was no valid message received
    else :
        # generate error message
        response = "ERROR : Invalid Message: Please Resend... "
        # attempt to retreive receive from address for error reply
        next_addr = address
        # pickle the error message
        try :
            message = pickle.dumps(response)
        except pickle.PickleError :
            error_message = "ERROR: Pickling the Response : "
            print (error_message)
            exc = sys.exc_info()[1]
            print (exc)





    # send the message
    try :
        bytes_sent = sock.sendto(message, next_addr)
    except OSError :
        error_message = "ERROR: Sending Requested Value : "
        print (error_message)
        exc = sys.exc_info()[1]
        print (exc)
    finally :
        # display message sent
        print ('\nsent {} bytes to {}'.format(bytes_sent, next_addr))
        print ('response sent : ' + str(response))




# close socket and exit program
sock.close
sys.exit()

#   eof
