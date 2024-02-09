import rpyc
import os

config = {"sync_request_timeout": None}
monitor = rpyc.connect("localhost", 11112, config=config).root #colocar o ip do monitor

PORT = 8081
monitor.register(PORT)

class FileService(rpyc.Service):
    PORT=8081
    diretorio = f"uploads{PORT}"

    def on_connect(self, conn):
        # Cria o diretório se não existir
        if not os.path.exists(self.diretorio):
            os.makedirs(self.diretorio)

    def on_disconnect(self, conn):
        pass

    def exposed_upload_file(self, file_name, data):
        # Handle file upload request
        with open(f"{self.diretorio}/{file_name}", "ab") as file:
                file.write(data)
            
    def exposed_stream_file(self, file_name):
        # Handle streaming request
        print(f"Streaming video: {file_name}")
        return file(file_name)
    
def file(file_name):
    file = open(f"uploads{PORT}/{file_name}", "rb")
    while True:
        chunk = file.read(2**20)
        if not chunk:
            break
        yield chunk
    file.close()
    
import threading
import time

def periodicallyPingMonitor():
    while True:
        monitor.ping(PORT)
        time.sleep(0.1)

if __name__ == "__main__":
    periodicallyPingMonitorThread = threading.Thread(target=periodicallyPingMonitor)
    periodicallyPingMonitorThread.start()

    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(FileService, port=FileService.PORT, protocol_config={"sync_request_timeout": None})
    t.start()