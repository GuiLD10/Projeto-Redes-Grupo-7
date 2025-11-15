# fase1/rdt20.py
import socket
import time
from utils.packet import (
    criar_pacote_simples,
    validar_pacote_simples,
    TYPE_DATA, TYPE_ACK, TYPE_NAK
)
from utils.simulator import UnreliableChannel
from utils.logger import log

SERVER_ADDR = ("localhost", 4000)

class RDT20Sender:
    def __init__(self, channel: UnreliableChannel):
        self.channel = channel
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def enviar(self, msg: str):
        dados = msg.encode()
        pacote = criar_pacote_simples(TYPE_DATA, dados)
        while True:
            self.channel.enviar(self.socket, pacote, SERVER_ADDR)
            log(f"[ENVIADO] {msg}")
            self.socket.settimeout(2)
            try:
                resposta, _ = self.socket.recvfrom(1024)
                tipo, _, valido = validar_pacote_simples(resposta)
                if valido and tipo == TYPE_ACK:
                    log("[ACK recebido] prosseguindo...")
                    break
                else:
                    log("[NAK recebido] retransmitindo...")
            except socket.timeout:
                log("[TIMEOUT] retransmitindo...")

class RDT20Receiver:
    def __init__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(SERVER_ADDR)
        log(f"[SERVIDOR] Escutando em {SERVER_ADDR}")

    def iniciar(self):
        while True:
            pacote, addr = self.socket.recvfrom(2048)
            tipo, dados, valido = validar_pacote_simples(pacote)
            if not valido:
                log("[ERRO] Pacote corrompido — enviando NAK")
                resposta = criar_pacote_simples(TYPE_NAK, b'')
            else:
                log(f"[RECEBIDO] {dados.decode()}")
                resposta = criar_pacote_simples(TYPE_ACK, b'')
            self.socket.sendto(resposta, addr)
