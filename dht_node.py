#   Vic Chimenti
#   CPSC 5510 FQ18
#   p2 Distributed Hash Table
#   dht_node.py
#   created         11/26/2018
#   last modified   11/27/2018


#   Distributed Hash Table Node




import socket
import sys




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

UDP_IP = "127.0.0.1"
UDP_PORT = 10109
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((UDP_IP, UDP_PORT))
while True:
 data, addr = sock.recvfrom(1024)
 print "received message:", data



















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
