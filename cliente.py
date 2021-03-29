import socket
import sys
import struct

"""
python cliente.py 127.0.0.1:5004 1,3,4,5,9
"""

s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)

host, port = sys.argv[1].split(":")
host= str(host)
port= int(port)
peer_list = []
log = open("output-IP.log",'w')

# msg=sys.argv[1]

# msg=msg.encode('utf-8')

input_chunks = sys.argv[2]
msg = "1 " +str(int(len(input_chunks)/2)+1)+" "+ input_chunks.replace(',',' ')
print(msg)
# msg = msg.encode('utf-8')
split = msg.split(' ')
tipo_msg = int(split[0])
if (tipo_msg == 1):
    qc = int(split[1])
    i =0
    chunk_list = []
    for i in range(qc):
        chunk_list.append(int(split[i+2]))
    print(chunk_list)
    pacote = struct.pack(('!HH'+ ('H' * len(chunk_list))), tipo_msg, qc, *[x for x in chunk_list])
    s.sendto(pacote,(host,port))
    print("Esperando resposta dos peers com Chunk Info...")
    try:
        while 1:
            #tempo de espera para receber chunkinfos
            s.settimeout(5)
            data, servaddr = s.recvfrom(1024)
            msg = struct.unpack(('!HH'+ ('H' * int((len(data)-4)/2))), data)
            print("CHUNKINFO RECEBIDO:", msg,servaddr)

            #CHUNKINFO RECEBIDO
            if(msg[0] == 3):
                peer_list.append((servaddr,list(msg[2:])))
                
    except :
        pass
    try:
        print("Timeout Chunk Info")
        s.settimeout(5)
        print("Peer List: ", peer_list)
        print("Procurando chunks desejados nas infos apresentadas...")
        for peer in peer_list:
            
            wanted_chunks = [value for value in chunk_list if value in peer[1]]
            if(wanted_chunks):
                #Get
                print("Encontrado um ou mais Chunks de interesse, enviando GET para ",peer[0],"\n Chunks: ",wanted_chunks)
                #remove os elementos da lista de wanted, tecnicamente deveria ser feito no response?
                chunk_list = [x for x in chunk_list if x not in wanted_chunks]
                pacote = struct.pack(('!HH'+ ('H' * len(wanted_chunks))), 4, len(wanted_chunks), *[x for x in wanted_chunks])
                s.sendto(pacote,peer[0])

                #Response
                for element in wanted_chunks:
                    data, servaddr = s.recvfrom(2048)
                    response = struct.unpack('!H100sH1024s',data)
                    chunkID = response[1].decode('utf-8').replace('\x00','')
                    output = str(servaddr[0])+":"+str(servaddr[1])+" - "+chunkID+"\n"
                    
                    chunk_data = response[3]
                    print("Response recebido com chunk",chunkID)
                    chunk = open(chunkID,'wb')
                    
                    chunk.write(chunk_data)
                    log.write(output)
                    print(output)
                    chunk.close()

    except:
        print("Timeout Get")
        pass
log.close()
