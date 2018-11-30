#   Vic Chimenti
#   CPSC 5510 FQ18
#   p2 Distributed Hash Table
#   dht_node.py
#   created         11/26/2018
#   last modified   11/29/2018
#   Distributed Hash Table Node
#   /usr/local/python3/bin/python3




import sys              # for system calls
import socket           # for udp socket functionality
import pickle           # for sending a list over socket
import argparse         # for parsing command line arguments




#   ***************     function definitions     ***************   #


# get the address and port for next node from hosfile contents
def getPath(c, v) :
    host_addr, host_port_str = c[v].split()
    host_port = int(host_port_str)
    node_addr = host_addr, host_port

    return node_addr


# extract the request attributes
def getRequest(r) :
    op = str(r[2])
    k = str(r[3])
    v = int(r[4])

    return op, k, v


# extract the client address from the request
def getClient(r) :
    a = str(r[0])
    p = int(r[1])
    cli = (a, p)

    return cli




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
charset = "UTF-8"                       # default encoding protocol
UDP_PORT = 10109


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

# get the socket address and port number from file contents
listen = getPath(content, args.linenum[0])
#host_addr, host_port_str = content[args.linenum[0]].split()
#host_port = int(host_port_str)

# create a udp socket
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(listen)
print ("Listening on Address, Port : " + str(listen))




# listen for communication
while True :
    message, address = sock.recvfrom(4096)
    request = pickle.loads(message)
    print ('received {} bytes from {}'.format(len(message), address))
    print ('request : ' + str(request))

    # assign request components to local varariables
    operation, key, value = getRequest(request)

    if value != args.linenum[0] :
        next_addr = getPath(content, value)
    else :
        next_addr = getClient(request)


    if message :
        message = pickle.dumps(request)
        bytes_sent = sock.sendto(message, next_addr)
        print ('sent {} bytes to {}'.format(bytes_sent, next_addr))

# TODO:
#   send address to cs2 instance of dht_node and have that instance respond
#   use hostfile to find cs2 instance of dht_node





















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
