import socket
import json
file_input = open('/home/koul/275_version_check/222/while-true/data/test.txt') #opens a file in reading mode
data = file_input.read(1024) #read 1024 bytes from the input file
bytesToSend = str.encode(data)
print(bytesToSend)
serverAddressPort = ("169.105.246.9", 21000)
bufferSize = 1024
# Create a UDP socket at client side
UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
# Send to server using created UDP socket
print(serverAddressPort)
dict = {"169.105.246.9":7929}
message=json.dumps({"IPaddress":"169.105.246.9","gossip":False, "Dictionary":dict, "BlackListedNodes":[]})
UDPClientSocket.sendto(message.encode(),serverAddressPort)

#msgFromServer = UDPClientSocket.recvfrom(bufferSize)
#msg = "Message from Server {}".format(msgFromServer[0])