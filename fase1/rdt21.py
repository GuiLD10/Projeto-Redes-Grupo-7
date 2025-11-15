# fase1/rdt21.py
import socket
import time
from utils.packet import criar_pacote, validar_pacote, TYPE_DATA, TYPE_ACK, TYPE_NAK
from utils.simulator import UnreliableChannel
from utils.logger import log

SERVER_ADDR = ("localhost", 4001)


# Remetente
class RDT21Sender:
    def __init__(self, channel: UnreliableChannel):
        self.channel = channel
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.seqnum = 0  # alterna 0 ↔ 1

    def enviar(self, msg: str):
        dados = msg.encode()
        pacote = criar_pacote(TYPE_DATA, self.seqnum, dados)

        while True:
            self.channel.enviar(self.socket, pacote, SERVER_ADDR)
            log(f"[ENVIADO seq={self.seqnum}] {msg}")
            self.socket.settimeout(2)

            try:
                resposta, _ = self.socket.recvfrom(1024)
                tipo, ack_seq, _, valido = validar_pacote(resposta)

                if not valido:
                    log("[ACK corrompido] retransmitindo...")
                    continue

                if tipo == TYPE_ACK and ack_seq == self.seqnum:
                    log(f"[ACK válido seq={ack_seq}] prosseguindo...")
                    self.seqnum = 1 - self.seqnum  # alterna 0/1
                    break
                else:
                    log("[ACK duplicado/incorreto] retransmitindo...")

            except socket.timeout:
                log("[TIMEOUT] retransmitindo...")

# Receptor
class RDT21Receiver:
    def __init__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(SERVER_ADDR)
        self.expected_seq = 0
        log(f"[SERVIDOR] Escutando em {SERVER_ADDR}")

    def iniciar(self):
        while True:
            pacote, addr = self.socket.recvfrom(2048)
            tipo, seqnum, dados, valido = validar_pacote(pacote)

            if not valido:
                log("[ERRO] Pacote corrompido — enviando NAK")
                resposta = criar_pacote(TYPE_NAK, self.expected_seq, b'')
                self.socket.sendto(resposta, addr)
                continue

            # Se for o pacote esperado
            if seqnum == self.expected_seq:
                log(f"[RECEBIDO seq={seqnum}] {dados.decode()}")
                resposta = criar_pacote(TYPE_ACK, seqnum, b'')
                self.socket.sendto(resposta, addr)
                self.expected_seq = 1 - self.expected_seq
            else:
                log(f"[DUPLICADO seq={seqnum}] reenviando ACK anterior")
                ack_anterior = criar_pacote(TYPE_ACK, 1 - self.expected_seq, b'')
                self.socket.sendto(ack_anterior, addr)
