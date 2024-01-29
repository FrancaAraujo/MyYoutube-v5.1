import rpyc

monitor = rpyc.connect("localhost", 11111).root

class LoadService(rpyc.Service):
    def __init__(self):
        self.datanode_index = 0  # Índice para implementar round robin

    def on_disconnect(self, conn):
        pass

    def exposed_get_next_datanodes(self):
        self.datanode_list = monitor.list_active()
        self.datanode_list = [(ip_port.split(':')[0], int(ip_port.split(':')[1])) for ip_port in self.datanode_list]
        print(self.datanode_list)
        # Escolhe os próximos 3 DataNodes usando round robin
        selected_datanodes = []
        for _ in range(min(3, len(self.datanode_list))):
            selected_node = self.datanode_list[self.datanode_index]
            selected_datanodes.append(selected_node)
            
            # Atualiza o índice para o próximo DataNode
            self.datanode_index = (self.datanode_index + 1) % len(self.datanode_list)
        
        return selected_datanodes
    
    def exposed_alive_from_list(self, list):
        return monitor.alive_from_list(list)

# Inicialização do servidor...
if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(LoadService, port=12222)
    t.start()