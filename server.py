import rpyc
import threading
import socket

HOST = "0.0.0.0"
PORT = 9999

def handle_client(client_socket):
    config = {"sync_request_timeout": None}
    conn = rpyc.connect("localhost", 10000, config=config)  # Conexão com o DataManager
    request = client_socket.recv(2**20).decode()
    client_socket.send("PRONTO".encode())

    if request.startswith("UPLOAD "):
        # Recebe o tamanho do vídeo
        video_size = int.from_bytes(client_socket.recv(8), byteorder='big')
        file_name = request[7:]
        print(f"Receiving file: {file_name}")
        
        def video_generator():
            chunk_size = 2**20
            received_size = 0
            while received_size < video_size:
                print(f"{received_size} {video_size}")
                chunk = client_socket.recv(chunk_size)
                print(len(chunk))
                if not chunk:
                    print("cheguei mas buguei")
                    break
                received_size += len(chunk)
                yield chunk
        
        conn.root.upload_file(file_name, video_generator())
        print(f"File '{file_name}' received and saved.")

    elif request.startswith("STREAM "):
        video_file = request[7:]
        print(f"Streaming video: {video_file}")

        video_data = conn.root.stream_file(video_file)
        for data_chunk in video_data:
            print("entrei")
            client_socket.sendall(data_chunk)
        client_socket.close()
        print(f"Video '{video_file}' streamed.")

    elif request.startswith("LISTAR "):
        files_list = conn.root.list_files()
        client_socket.send(','.join(files_list).encode('utf-8'))
        client_socket.close()

    elif request.startswith("SEARCH "):
        file_name = request[7:]
        search_results = conn.root.search_files(file_name)
        client_socket.send(','.join(search_results).encode('utf-8'))
        client_socket.close()

    conn.close()

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind((HOST, PORT))
server.listen()
print("Server listening for incoming connections...")

while True:
    client, addr = server.accept()
    client_handler = threading.Thread(target=handle_client, args=(client,))
    client_handler.start()