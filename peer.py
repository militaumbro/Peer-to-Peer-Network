import socket
import struct
import sys
"""
./peer 127.0.0.1:5001 key-values-files_peer1 127.0.0.1:5003 127.0.0.1:5002
"""
host, port = sys.argv[1].split(":")
host= str(host)
port= int(port)

my_chuncks = []
my_neighbours = []
key_value_file = sys.argv[2]

for address in sys.argv[3:]:
    neyIp, neyPort = address.split(":")
    my_neighbours.append((str(neyIp),int(neyPort)))

print(host,port,my_neighbours)

s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)

s.bind(("",port))

print("waiting on port:", port)


key_file = open(key_value_file,'r')

lines = key_file.read().splitlines()

for line in lines:
    split = line.split(':')
    chunk = (int(split[0]),split[1].strip())
    my_chuncks.append(chunk)

print("Linhas: ",my_chuncks)


while 1:
    while 1:

        data, clientaddr= s.recvfrom(1024)
        print(int((len(data)-4)/2))
        try:
            msg = struct.unpack(('!HH'+ ('H' * int((len(data)-4)/2))), data)
            print(msg)
        except:
            print(sys.exc_info())
            pass
        # data=data.decode('utf-8')
        #  = struct.unpack(('!HH'+('H'* len(bytes))), bytes)
        break
    
    # Hello
    if(msg[0] == 1):

        reply = struct.pack(('!HH'+ ('H' * len(my_chuncks))), 3, len(my_chuncks), *[x[0] for x in my_chuncks])
        print(3, len(my_chuncks),*[x[0] for x in my_chuncks])
        s.sendto(reply,clientaddr)
        print("chunks: ",msg[2:])
        #Send Query to neighbors
        for neyghbor_address in my_neighbours:
            pacote = struct.pack(('!H20sHHH'+ ('H' * msg[1])),2,clientaddr[0].encode('utf-8'),clientaddr[1],3,msg[1],*[x for x in msg[2:]])
            s.sendto(pacote,neyghbor_address)

    # Query
    if(msg[0] == 2):
        print("Recebido Query")
        query = struct.unpack(('!H20sHHH'+ ('H' * int((len(data)-28)/2))), data)
        print(query)

        #chunkinfo para cliente
        reply = struct.pack(('!HH'+ ('H' * len(my_chuncks))), 3, len(my_chuncks), *[x[0] for x in my_chuncks])
        print(3, len(my_chuncks),*[x[0] for x in my_chuncks])
        client_ip = query[1].decode('utf-8').replace('\x00','')
        client_port = int(query[2])
        print("Client address: ",client_ip,client_port)
        s.sendto(reply,(client_ip,client_port))
        

        #diminui o TTL
        newQuery =(*query[:3],int(query[3])-1,*query[4:])
        print(newQuery)
        if(newQuery[3] > 0):
            for neyghbor_address in my_neighbours:
                # se o vizinho nao for quem me mandou a query repasse para ele
                if (neyghbor_address != clientaddr):
                    pacote = struct.pack(('!H20sHHH'+ ('H' * int((len(data)-28)/2))),*newQuery)
                    s.sendto(pacote,neyghbor_address)

    



    #Recebi Get
    if(msg[0] == 4):
        print("Get Recebido de: ", clientaddr)
        chunk_indexes = [x[0] for x in my_chuncks]
        print(chunk_indexes)
        for i in range(int(msg[1])):
            
            index = chunk_indexes.index(msg[i+2])
            #se existe um chunk com o valor pedido pela query
            if index >= 0:
                print("Arquivo do Chunk ",msg[i+2]," ",my_chuncks[index][1])
                print("Preparando para enviar Response...")
                
                file = open(my_chuncks[index][1],'rb')
                data = file.read(1024)
                response = struct.pack('!H100sH1024s',5,my_chuncks[index][1].encode('utf-8'),len(data),data)
                s.sendto(response,clientaddr)
                print("Enviado")
                file.close()
key_file.close()
                

