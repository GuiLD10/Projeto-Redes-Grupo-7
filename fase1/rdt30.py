# fase1/rdt30.py
import socket
import time
from utils.packet import criar_pacote, validar_pacote, TYPE_DATA, TYPE_ACK, TYPE_NAK
from utils.simulator import UnreliableChannel
from utils.logger import log

SERVER_ADDR = ("localhost", 4002)
TIMEOUT_INITIAL = 2.0  # 2 segundos conforme enunciado

class RDT30Sender:
    def __init__(self, channel: UnreliableChannel, timeout=TIMEOUT_INITIAL):
        self.channel = channel
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.seqnum = 0
        self.timeout = timeout

        # métricas
        self.retransmissions = 0
        self.bytes_sent = 0
        self.start_time = None
        self.end_time = None

    def enviar(self, data_bytes: bytes):
        """
        Envia um único bloco de dados (stop-and-wait).
        """
        pacote = criar_pacote(TYPE_DATA, self.seqnum, data_bytes)
        self.bytes_sent += len(data_bytes)
        # início do envio (marca tempo apenas na primeira chamada)
        if self.start_time is None:
            self.start_time = time.time()

        while True:
            send_time = time.time()
            # usar channel.enviar com is_ack=False
            self.channel.enviar(self.socket, pacote, SERVER_ADDR, is_ack=False)
            log(f"[ENVIADO seq={self.seqnum}] {data_bytes[:50]!r} (len={len(data_bytes)})")
            # aguardando ACK dentro do timeout, usando recvfrom com pequeno timeout para checar
            remaining = self.timeout
            self.socket.settimeout(0.2)  # pequeno poll
            got_ack = False
            while time.time() - send_time < self.timeout:
                try:
                    resposta, _ = self.socket.recvfrom(4096)
                except socket.timeout:
                    continue
                # valida a resposta
                try:
                    tipo, ack_seq, _, valido = validar_pacote(resposta)
                except Exception:
                    # pacote de ACK com formato inesperado -> ignorar
                    continue

                # se inválido, ignorar e continuar esperando
                if not valido:
                    log("[ACK corrupto recebido] ignorando e aguardando...")
                    continue

                # se for ACK e seq esperado -> sucesso
                if tipo == TYPE_ACK and ack_seq == self.seqnum:
                    log(f"[ACK recebido seq={ack_seq}] prosseguindo...")
                    got_ack = True
                    break
                # se NAK ou seq incorreto -> ignora e aguarda timeout para retransmitir
                if tipo == TYPE_NAK:
                    log("[NAK recebido] retransmitindo imediatamente...")
                    break

            if got_ack:
                # avan�a seq e retorna
                self.seqnum = 1 - self.seqnum
                break
            else:
                # timeout ou NAK -> retransmitir
                self.retransmissions += 1
                log("[TIMEOUT/NAK] retransmitindo pacote...")

        # marca fim do envio desta mensagem (não necessariamente fim de todo o fluxo)
        # atualizamos end_time a cada entrega bem sucedida
        self.end_time = time.time()

    def stats(self):
        total_time = (self.end_time - self.start_time) if (self.start_time and self.end_time) else 0.0
        throughput = (self.bytes_sent / total_time) if total_time > 0 else 0.0
        return {
            "retransmissions": self.retransmissions,
            "bytes_sent": self.bytes_sent,
            "total_time_s": total_time,
            "throughput_Bps": throughput
        }


class RDT30Receiver:
    def __init__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(SERVER_ADDR)
        self.expected_seq = 0
        log(f"[SERVIDOR] Escutando em {SERVER_ADDR}")

    def iniciar(self):
        while True:
            pacote, addr = self.socket.recvfrom(8192)
            try:
                tipo, seqnum, dados, valido = validar_pacote(pacote)
            except Exception:
                # pacote mal formado -> ignorar
                continue

            if not valido:
                log("[ERRO] Pacote corrompido — enviando NAK")
                nak = criar_pacote(TYPE_NAK, self.expected_seq, b'')
                # enviar NAK usando channel? aqui não temos channel; enviamos direto (test harness usará UnreliableChannel ao enviar)
                # para compatibilidade com a simulação: servidor envia NAK diretamente (test harness pode configurar perda de ACKs)
                self.socket.sendto(nak, addr)
                continue

            if seqnum == self.expected_seq:
                log(f"[RECEBIDO seq={seqnum}] {dados.decode(errors='replace')}")
                ack = criar_pacote(TYPE_ACK, seqnum, b'')
                self.socket.sendto(ack, addr)
                self.expected_seq = 1 - self.expected_seq
            else:
                log(f"[DUPLICADO seq={seqnum}] reenviando ACK anterior")
                # reenviar ACK do último pacote entregue (seq anterior)
                ack_prev = criar_pacote(TYPE_ACK, 1 - self.expected_seq, b'')
                self.socket.sendto(ack_prev, addr)
