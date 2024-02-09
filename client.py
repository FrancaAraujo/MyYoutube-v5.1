from flask import Flask, render_template, request, Response
import socket

HOST = "192.168.100.5" #id do server
PORT = 9999

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/listar', methods=['POST', 'GET'])
def listar_arquivos():
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((HOST, PORT))

    client.send(f"LISTAR ".encode())  # Use the "STREAM" request format
    client.recv(2**20)

    # Receba a lista de nomes de arquivo do servidor
    data = client.recv(2**20).decode('utf-8')
    nomes_arquivos = data.split(',')

    client.close()
    return render_template('index2.html', nomes_arquivos=nomes_arquivos)


@app.route('/search', methods=['POST', 'GET'])
def search_arquivos():
    file_name = request.form['video_name']
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((HOST, PORT))

    client.send(f"SEARCH {file_name}".encode())
    client.recv(2**20)

    # Receba a lista de nomes de arquivo do servidor
    data = client.recv(2**20).decode('utf-8')
    nomes_arquivos = data.split(',')

    client.close()
    return render_template('index3.html', nomes_arquivos=nomes_arquivos)


@app.route('/upload', methods=['POST'])
def upload():
    file = request.files['file']
    file_name = file.filename
    print(file_name)
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((HOST, PORT))
    client.send(f"UPLOAD {file_name}".encode())

    chunk_size = 2**20  # Tamanho do chunk (pode ser ajustado conforme necessário)
    client.recv(chunk_size)
    data = file.read()
    # Envia o tamanho do vídeo antes dos dados reais
    client.sendall(len(data).to_bytes(8, byteorder='big'))

    # Envia o vídeo em chunks
    chunk_size = 2**20
    for i in range(0, len(data), chunk_size):
        client.sendall(data[i:i+chunk_size])
    # Enviar o último chunk, se houver
    if i < len(data):
        client.sendall(data[i:])
    client.close()

    return "File uploaded successfully! You can now upload another file."


@app.route('/stream')
def stream():
    video_name = request.args.get('id')
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((HOST, PORT))
    client.send(f"STREAM {video_name}".encode())  # Use the "STREAM" request format
    client.recv(2**20)

    def generate(client):
        chunk_size = 2**20  # Tamanho dos pedaços em "bytes"
        while True:
            data = client.recv(chunk_size)
            if not data:
                break
            yield data
        client.close()

    return Response(generate(client), content_type='video/mp4')


if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=False, port=5001)
