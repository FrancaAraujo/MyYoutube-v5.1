# base de dados distribuída
import rpyc
import datetime
import os

class MonitorService(rpyc.Service):
    def on_connect(self, conn):
        ip, port = conn._channel.stream.sock.getpeername()
        self.ip = ip
        self.port = port
        print("IP = ", ip, " Porta = ", port)

    def on_disconnect(self, conn):
        pass

    # Registra o Nó para monitoramento
    def exposed_register(self, clientServicePort):
        register(self.ip, clientServicePort)
        print("IP = ", self.ip, " Porta = ", clientServicePort)

    # Alerta o serviço de monitoramento que o Nó ainda está ativo
    def exposed_ping(self, clientServicePort):
        ping(self.ip, clientServicePort)
        print("IP = ", self.ip, " Porta = ", clientServicePort)
    
    # Lista de todos os Nós atualmente ativos
    def exposed_list_active(self):
        return list_active()

    # Verifica se o Nó está ativo
    def exposed_is_alive(self, address):
        return is_alive(address)

    # Recebe uma lista de Nós e retorna seu subconjunto de todos os Nós ativos
    def exposed_alive_from_list(self, list):
        return alive_from_list(list)
    
hosts_data = {}

def register(ip, port):
    address = str(ip) + ":" + str(port)
    print(f"Registrando novo Nó {address}")
    hosts_data[address] = datetime.datetime.now()

def ping(ip, port):
    address = str(ip) + ":" + str(port)
    if address not in hosts_data:
        print("ERRO: ENDEREÇO {} NÃO REGISTRADO".format(address))
        return -1

    print(f"ping {address}")

    hosts_data[address] = datetime.datetime.now()


def is_alive(address):
    if address in hosts_data and (datetime.datetime.now() - hosts_data[address]) < datetime.timedelta(seconds=20):
        return True
    else: 
        return False

def list_active():
        active_list = [address for address in hosts_data if is_alive(address)]
        return active_list

def alive_from_list(address_list):
    return [address for address in address_list if is_alive(address)]

#Implementar "pub-sub" para mandar toda hora para o loadbalance quem está vivo e quem morreu

# Inicializa o servidor de objeto remoto e o registra no serviço de nomes
if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(MonitorService, port=11111, auto_register=True, protocol_config={"allow_public_attrs": True})
    t.start()
