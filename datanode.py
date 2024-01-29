import rpyc
import os


monitor = rpyc.connect("localhost", 11111).root
PORT = 8083
monitor.register(PORT)

class FileService(rpyc.Service):
    PORT=8083
    diretorio = f"uploads{PORT}"

    def on_connect(self, conn):
        # Cria o diretório se não existir
        if not os.path.exists(self.diretorio):
            os.makedirs(self.diretorio)

    def on_disconnect(self, conn):
        pass

    def exposed_upload_file(self, file_name, data):
        # Handle file upload request
        print(f"Receiving file: {file_name}")
        with open(f"{self.diretorio}/{file_name}", "ab") as file:
            file.write(data)
        print(f"File '{file_name}' received and saved.")

    def exposed_stream_file(self, file_name):
        # Handle streaming request
        print(f"Streaming video: {file_name}")
        with open(f"{self.diretorio}/{file_name}", "rb") as video_file:
            video_data = video_file.read()
        print(f"Video '{file_name}' streamed.")
        return video_data
    
import threading
import time

def periodicallyPingMonitor():
    while True:
        monitor.ping(PORT)
        time.sleep(15)

if __name__ == "__main__":
    periodicallyPingMonitorThread = threading.Thread(target=periodicallyPingMonitor)
    periodicallyPingMonitorThread.start()

    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(FileService, port=FileService.PORT)
    t.start()