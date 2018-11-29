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
import argparse         # for parsing command line arguments




#   ***************     function definitions     ***************   #


# define the size of the table
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




UDP_IP = "127.0.0.1"
UDP_PORT = 10109


# parse and assign command-line input
parser = argparse.ArgumentParser()
parser.add_argument('hostfile', type=str, nargs=1, required=True)
parser.add_argument('linenum', type=int, nargs=1, required=True)
args = parser.parse_args()


# create a udp socket
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((UDP_IP, UDP_PORT))


# listen for communication
while True:
 message, address = sock.recvfrom(4096)
 print ('received {} bytes from {}'.format(len(message), address))
 print ('message : ' + message)

 if message :
     bytes_sent = sock.sendto(message, address)
     print ('sent {} bytes back to {}'.format(bytes_sent, address))




















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
