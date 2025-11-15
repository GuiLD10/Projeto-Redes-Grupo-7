# fase2/sr.py
import socket
import threading
import time
from utils.packet import criar_pacote_fase2, validar_pacote_fase2, TYPE_DATA, TYPE_ACK
from utils.simulator import UnreliableChannel
from utils.logger import log

SERVER_ADDR = ("localhost", 5001)

class SR_Sender:
    def __init__(self, channel: UnreliableChannel, window_size=5, timeout=2.0):
        self.channel = channel
        self.window_size = window_size
        self.timeout = timeout * 2
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.lock = threading.Lock()

        self.acks_recebidos = {}
        self.total_pacotes = 0
        self.retransmissoes = 0


        # Controle de janela
        self.base = 0
        self.nextseqnum = 0
        self.buffer = {}  # seqnum -> pacote
        self.acks = {}    # seqnum -> bool
        self.timers = {}  # seqnum -> threading.Timer

        # Métricas
        self.bytes_sent = 0
        self.retransmissions = 0
        self.start_time = None
        self.end_time = None
        

        threading.Thread(target=self._ouvir_acks, daemon=True).start()

    def enviar(self, data_bytes: bytes):
        """Divide os dados e envia com SR."""
        if self.start_time is None:
            self.start_time = time.time()

        segment_size = 1024
        chunks = [data_bytes[i:i+segment_size] for i in range(0, len(data_bytes), segment_size)]
        total_chunks = len(chunks)
        self.total_pacotes = len(chunks)

        log(f"[SR] Enviando {total_chunks} segmentos com janela={self.window_size}")

        idx = 0
        while idx < total_chunks or self.base < self.nextseqnum:
            with self.lock:
                # Envia enquanto houver espaço na janela
                while idx < total_chunks and self.nextseqnum < self.base + self.window_size:
                    pacote = criar_pacote_fase2(TYPE_DATA, self.nextseqnum, chunks[idx])
                    self.buffer[self.nextseqnum] = pacote
                    self.acks[self.nextseqnum] = False
                    self.channel.enviar(self.socket, pacote, SERVER_ADDR, is_ack=False)
                    log(f"[ENVIADO seq={self.nextseqnum}] ({len(chunks[idx])} bytes)")
                    self.bytes_sent += len(chunks[idx])
                    self._iniciar_timer(self.nextseqnum)
                    self.nextseqnum += 1
                    idx += 1

                # Move janela base se possível
                while self.acks.get(self.base, False):
                    self._parar_timer(self.base)
                    del self.acks[self.base]
                    del self.buffer[self.base]
                    self.base += 1

            time.sleep(0.01)

        # Aguarda todos os ACKs
        tempo_inicio_espera = time.time()
        tempo_max_espera = 5  # segundos extras de espera final

        while True:
            with self.lock:
                todos_confirmados = all(self.acks_recebidos.get(seq, False) for seq in range(self.total_pacotes))
            if todos_confirmados:
                break
            if time.time() - tempo_inicio_espera > tempo_max_espera:
                print("[AVISO] Tempo máximo de espera atingido — encerrando transmissão.")
                break
            time.sleep(0.1)

        self.end_time = time.time()
        self.socket.close()
        print(f"[✓] Transmissão finalizada — {self.total_pacotes} pacotes, {self.retransmissoes} retransmissões.")


    def _iniciar_timer(self, seqnum):
        t = threading.Timer(self.timeout, self._timeout_event, [seqnum])
        self.timers[seqnum] = t
        t.start()

    def _parar_timer(self, seqnum):
        t = self.timers.get(seqnum)
        if t:
            t.cancel()
            del self.timers[seqnum]

    def _timeout_event(self, seqnum):
        with self.lock:
            if not self.acks.get(seqnum, False):
                log(f"[TIMEOUT seq={seqnum}] retransmitindo...")
                pacote = self.buffer.get(seqnum)
                if pacote:
                    self.channel.enviar(self.socket, pacote, SERVER_ADDR, is_ack=False)
                    self.retransmissoes += 1
                    # reinicia timer somente se ainda não confirmado
                    if not self.acks.get(seqnum, False):
                        self._iniciar_timer(seqnum)

    def _ouvir_acks(self):
        """Thread que escuta e processa os ACKs recebidos"""
        while True:
            try:
                resposta, _ = self.socket.recvfrom(4096)
                tipo, acknum, _, valido = validar_pacote_fase2(resposta)
                if not valido or tipo != TYPE_ACK:
                    continue

                with self.lock:
                    # Marca o ACK como recebido
                    self.acks_recebidos[acknum] = True

                    # Se o pacote ainda estiver pendente, confirma e para o timer
                    if acknum in self.acks and not self.acks[acknum]:
                        self.acks[acknum] = True
                        self._parar_timer(acknum)
                        log(f"[ACK recebido seq={acknum}] — confirmado e removido do buffer")

                        # Avança a janela base
                        while self.base in self.acks and self.acks[self.base]:
                            del self.acks[self.base]
                            del self.buffer[self.base]
                            self.base += 1

            except OSError:
                # Socket fechado: encerrar thread
                break
            except Exception as e:
                log(f"[ERRO ACKS] {e}")
                break


    def stats(self):
        total_time = (self.end_time - self.start_time) if (self.start_time and self.end_time) else 0
        throughput = self.bytes_sent / total_time if total_time > 0 else 0
        return {
            "bytes_sent": self.bytes_sent,
            "retransmissions": self.retransmissions,
            "total_time_s": total_time,
            "throughput_Bps": throughput
        }


class SR_Receiver:
    def __init__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(SERVER_ADDR)
        self.window_size = 5
        self.rcv_base = 0
        self.buffer = {}  # seqnum -> dados
        log(f"[SERVIDOR SR] Escutando em {SERVER_ADDR}")

    def iniciar(self, channel: UnreliableChannel):
        try:
            while True:
                pacote, addr = self.socket.recvfrom(8192)
                tipo, seqnum, dados, valido = validar_pacote_fase2(pacote)
                if not valido:
                    log("[ERRO] Pacote corrompido — descartando")
                    continue

                # Pacote dentro da janela
                if self.rcv_base <= seqnum < self.rcv_base + self.window_size:
                    log(f"[RECEBIDO seq={seqnum}] ({len(dados)} bytes)")
                    self.buffer[seqnum] = dados
                    ack = criar_pacote_fase2(TYPE_ACK, seqnum, b'')
                    channel.enviar(self.socket, ack, addr, is_ack=True)

                    # entrega em ordem + ACKs consecutivos
                    while self.rcv_base in self.buffer:
                        del self.buffer[self.rcv_base]
                        ack = criar_pacote_fase2(TYPE_ACK, self.rcv_base, b'')
                        channel.enviar(self.socket, ack, addr, is_ack=True)
                        log(f"[ENTREGUE seq={self.rcv_base}] ACK enviado")
                        self.rcv_base += 1

                else:
                    # fora da janela
                    ultimo_ack = max(self.buffer.keys()) if self.buffer else self.rcv_base - 1
                    log(f"[FORA DE JANELA seq={seqnum}] reenviando ACK {ultimo_ack}")
                    ack = criar_pacote_fase2(TYPE_ACK, ultimo_ack, b'')
                    channel.enviar(self.socket, ack, addr, is_ack=True)

        except KeyboardInterrupt:
            pass
        finally:
            self.socket.close()
            print("[SERVIDOR] Finalizado corretamente.")
