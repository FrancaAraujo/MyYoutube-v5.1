import rpyc
import threading
import socket

HOST = "0.0.0.0"
PORT = 9999

def handle_client(client_socket):
    conn = rpyc.connect("localhost", 10000)  # Conexão com o DataManager

    request = client_socket.recv(2**20).decode()


    if request.startswith("UPLOAD "):
        file_name = request[7:]
        print(f"Receiving file: {file_name}")

        done = False
        temp = b""
        while not done:
            data = client_socket.recv(1024)
            if data[-5:] == b"<END>":
                done = True
                conn.root.upload_file(file_name, data[:-5])
            else:
                conn.root.upload_file(file_name, data)
                
        print(f"File '{file_name}' received and saved.")

    elif request.startswith("STREAM "):
        video_file = request[7:]
        print(f"Streaming video: {video_file}")

        video_data = conn.root.stream_file(video_file)
        if video_data:
            client_socket.sendall(video_data)
            client_socket.send(b"<END>")
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