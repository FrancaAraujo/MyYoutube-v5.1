import rpyc
from rpyc.utils.server import ThreadedServer
import os
import time

config = {"sync_request_timeout": None}
loadbalance = rpyc.connect("localhost", 12222, config=config).root

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
        print(chosen_datanodes)
        for data_chunk in data:
            print("entrei")
            for ip, port in chosen_datanodes:
                config = {"sync_request_timeout": None}
                conn = rpyc.connect(ip, port, config=config)
                storage_service = conn.root
                storage_service.upload_file(file_name, data_chunk)
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
        print(selected_datanode)
        return selected_datanode

    def exposed_stream_file(self, file_name):
        print(f"Streaming video: {file_name}")
        return self.download(file_name)

    def exposed_list_files(self):
        print("HEHE")
        return list(self.index.keys())

    def exposed_search_files(self, search_query):
        return [file_name for file_name in self.index if search_query in file_name]

    def download(self, file_name, from_byte=0):
        while True:
            try:
                ip, port = self.get_next_datanode_for_streaming(file_name)
                config = {"sync_request_timeout": None}
                datanode_service = rpyc.connect(ip, port, config=config)
                new_byte = 0
                catching_up = True
                for chunk in datanode_service.root.stream_file(file_name):
                    if catching_up == True:
                        if new_byte == from_byte:
                            catching_up = False
                        else:
                            new_byte += len(chunk)
                    if catching_up == False:
                        from_byte += len(chunk)
                        yield chunk
                break
            except Exception as e:
                print("Failed while downloading file, trying to connect to another node")

if __name__ == "__main__":
    manager_server = ThreadedServer(DataNodeManagerService, port=10000, protocol_config={"sync_request_timeout": None})
    manager_server.start()