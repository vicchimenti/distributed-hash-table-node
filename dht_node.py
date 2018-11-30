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


# get the hostname
# def getHost() :
#     try :
#         h = socket.gethostname()
#     except AttributeError :
#         error_message = "ERROR Failed to Get Hostname"
#         print (error_message)
#         sys.exit ("Exiting Program")
#
#     return h
#
#
# # get the host IP number
# def getIP(h) :
#     try :
#         h_ip = socket.gethostbyname(h)
#     except AttributeError :
#         error_message = "ERROR Failed to Get Host IP Number"
#         print (error_message)
#         sys.exit ("Exiting Program")
#
#     return h_ip

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


# open file and assign
with open (args.hostfile[0], 'r') as file :
    content = file.readlines()
#host_addr = content[args.linenum[0]]
host_addr, host_port_str = content[args.linenum[0]].split()
host_port = int(host_ip_str)

# get the local host and ip address
# host = getHost()
# host_ip = getIP(host)


# create a udp socket
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(host_addr, host_port)
print ("Listening on Port : " + str(host_port))


# listen for communication
while True:
    message, address = sock.recvfrom(4096)
    msg = str(message.decode(charset))
    print ('received {} bytes from {}'.format(len(message), address))
    print ('message : ' + msg)

    if message :
     bytes_sent = sock.sendto(message, address)
     print ('sent {} bytes back to {}'.format(bytes_sent, address))

# TODO:
#   send address to cs2 instance of dht_node and have that instance respond
#   use hostfile to assign portno
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
