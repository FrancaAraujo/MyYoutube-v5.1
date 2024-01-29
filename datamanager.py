import rpyc
from rpyc.utils.server import ThreadedServer
import os

loadbalance = rpyc.connect("localhost", 12222).root

class DataNodeManagerService(rpyc.Service):
    def __init__(self):
        self.datanode_index = 0  # Inicializa o contador para o balanceamento de carga
        self.file_streaming_index = 0  # Contador para o balanceamento de carga no streaming
        self.index_file = "index.txt"
        self.ensure_index_file()
        self.index = self.load_index()

    def ensure_index_file(self):
        # Verifica se o arquivo 'index.txt' existe, se não, cria um novo
        if not os.path.exists(self.index_file):
            open(self.index_file, 'w').close()

    def load_index(self):
        index = {}
        with open(self.index_file, "r") as index_file:
            for line in index_file:
                file_name, ip, port = line.strip().split()
                index.setdefault(file_name, []).append((ip, int(port)))
        index_file.close()
        return index

    def exposed_upload_file(self, file_name, data):
        chosen_datanodes = loadbalance.get_next_datanodes()
        for ip, port in chosen_datanodes:
            conn = rpyc.connect(ip, port)
            conn.root.upload_file(file_name, data)
            conn.close()
        self.index[file_name] = chosen_datanodes
        with open("index.txt", "a") as index_file:
            for ip, port in chosen_datanodes:
                index_file.write(f"{file_name} {ip} {port}\n")
        index_file.close()

    def get_next_datanode_for_streaming(self, file_name):
        datanodes = self.index[file_name]
        datanodes = [f"{ip}:{port}" for ip, port in datanodes]
        livedatanodes = loadbalance.alive_from_list(datanodes)
        
        livedatanodes = [(ip_port.split(':')[0], int(ip_port.split(':')[1])) for ip_port in livedatanodes]
        
        # Verifica se há datanodes vivos
        if not livedatanodes:
            return None
        
        selected_datanode = []
        selected_datanode = livedatanodes[self.file_streaming_index]

        # Atualiza o contador para o próximo datanode
        self.file_streaming_index = (self.file_streaming_index + 1) % len(livedatanodes)
        
        return selected_datanode
    
    def exposed_stream_file(self, file_name):
        print(f"Streaming video: {file_name}")
        if file_name not in self.index:
            return None

        ip, port = self.get_next_datanode_for_streaming(file_name)
        conn = rpyc.connect(ip, port)
        video_data = conn.root.stream_file(file_name)
        conn.close()

        print(f"Video '{file_name}' streamed.")
        return video_data

    def exposed_list_files(self):
        return list(self.index.keys())

    def exposed_search_files(self, search_query):
        return [file_name for file_name in self.index if search_query in file_name]

if __name__ == "__main__":
    manager_server = ThreadedServer(DataNodeManagerService, port=10000)
    manager_server.start()