#   Vic Chimenti
#   CPSC 5510 FQ18
#   p2 Distributed Hash Table
#   dht_node.py
#   created         11/26/2018
#   last modified   11/30/2018
#   Distributed Hash Table Node
#   /usr/local/python3/bin/python3




import sys                  # for system calls
import socket               # for udp socket functionality
import pickle               # for sending a list over socket
import argparse             # for parsing command line arguments
import hashlib              # SHA1 hash functionality




#   ***************     function definitions     ***************   #


# get the address and port for next node from hosfile contents
# def getPath(c, v) :
#     host_addr, host_port_str = c[v].split()
#     host_port = int(host_port_str)
#     node_addr = host_addr, host_port
#
#     return node_addr


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
def hexID(a, p) :
    p = socket.htonl(p)
    mh = hashlib.sha1()
    mh.update(repr(a).encode(charset))
    mh.update(repr(p).encode(charset))

    return mh.hexdigest()


# calculate the node ID in hex
def getID(a, p) :
    p = socket.htonl(p)
    mh = hashlib.sha1()
    mh.update(repr(a).encode(charset))
    mh.update(repr(p).encode(charset))

    return mh.digest()



# # define the size of the table
# def distance(a, b):
#     return a^b
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




# define defaults
charset = "UTF-8"       # default encoding protocol
#my_node_ID = 99999      # default node ID as long
#UDP_PORT = 10109


# parse and assign command-line input
parser = argparse.ArgumentParser()
parser.add_argument('hostfile', type=str, nargs=1)
parser.add_argument('linenum', type=int, nargs=1)
args = parser.parse_args()

#  *** prints args as list elements *** #
print ('hostfile : ' + str(args.hostfile))
print ('linenum : ' + str(args.linenum))




# open file and assign to list
with open (args.hostfile[0], 'r') as file :
    content = file.readlines()

# create dictionary of the finger table
count = len(content.readlines())
print ('count : ' + str(count))
sys.exit()





# split addr port info of my node
host_addr, host_port = getPath(content, args.linenum[0])

# create a udp socket
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((host_addr, host_port))
print ("Listening on Address, Port : " + str((host_addr, host_port)))




# calculate my current node hash value
my_node_ID = hexID(host_addr, host_port)

# listen for communication
while True :
    message, address = sock.recvfrom(4096)
    request = pickle.loads(message)
    print ('received {} bytes from {}'.format(len(message), address))
    print ('request : ' + str(request))

    # if a valid message arrived
    if message :
        # assign request components to local varariables
        cli_addr, cli_port, hops, operation, key, value = getRequest(request)
        # increment each hop
        hops += 1

        # if the value matches current node return directly to the client
        if value == args.linenum[0] :
            next_addr = getClient(request)
            # return to client hash-key, hash-node, hops, key_str, value_str
            response = key, my_node_ID, hops, str(key), str(value)
        # or else get the address of the next node
        else :
            next_addr = getPath(content, value)
            # forward to next node hash-key, hash-node, hops, key_str, value_str
            response = cli_addr, cli_port, hops, operation, key, value

        # pickle the response and send
        message = pickle.dumps(response)
        bytes_sent = sock.sendto(message, next_addr)
        print ('sent {} bytes to {}'.format(bytes_sent, next_addr))





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
