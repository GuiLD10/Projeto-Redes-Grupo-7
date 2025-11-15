# testes/test_fase1.py
import threading
import time
from fase1.rdt20 import RDT20Sender, RDT20Receiver
from utils.packet import validar_pacote
from utils.simulator import UnreliableChannel
from fase1.rdt21 import RDT21Sender, RDT21Receiver
import threading, time
from fase1.rdt30 import RDT30Sender, RDT30Receiver
from utils.simulator import UnreliableChannel
from utils.packet import criar_pacote, TYPE_ACK, TYPE_NAK
from utils.logger import log
import socket

def test_rdt20():
    canal = UnreliableChannel(loss_rate_ack=0.0, corrupt_rate=0.3)
    receiver = RDT20Receiver()
    threading.Thread(target=receiver.iniciar, daemon=True).start()
    time.sleep(1)

    sender = RDT20Sender(canal)
    mensagens = [f"Mensagem {i}" for i in range(10)]
    for m in mensagens:
        sender.enviar(m)

    print("\n✓ Teste finalizado com sucesso.")

def test_rdt21():

    canal = UnreliableChannel(loss_rate_ack=0.0, corrupt_rate=0.2)
    receiver = RDT21Receiver()
    import threading, time
    threading.Thread(target=receiver.iniciar, daemon=True).start()
    time.sleep(1)

    sender = RDT21Sender(canal)
    mensagens = [f"Mensagem {i}" for i in range(10)]
    for m in mensagens:
        sender.enviar(m)

    print("\n✓ Teste RDT2.1 finalizado com sucesso.")

# testes/test_fase1.py  (acrescente)
def test_rdt30():
    """
    Teste rdt3.0:
    - perda de 15% em DATA
    - perda de 15% em ACKs
    - atraso 50-500ms
    - envia 20 mensagens curtas
    """

    # configurar canal: 15% perda DATA, 15% perda ACK, sem corrupção (ou deixe pequena)
    canal = UnreliableChannel(loss_rate_data=0.15,
                              loss_rate_ack=0.15,
                              corrupt_rate=0.0,
                              delay_range=(0.05, 0.5))

    # iniciando receptor (vamos modificá-lo ligeiramente em runtime para enviar ACKs via canal)
    receiver = RDT30Receiver()
    def receiver_worker():
        # abrimos um socket para envio via channel (mesmo socket que o receiver já tem)
        sock = receiver.socket
        while True:
            pacote, addr = sock.recvfrom(8192)
            try:
                tipo, seqnum, dados, valido = validar_pacote(pacote)
            except Exception:
                continue

            if not valido:
                log("[RECEPTOR] pacote corrompido -> enviar NAK via canal")
                nak = criar_pacote(TYPE_NAK, receiver.expected_seq, b'')
                # enviar NAK via channel com is_ack=True para aplicar loss_rate_ack
                canal.enviar(sock, nak, addr, is_ack=True)
                continue

            if seqnum == receiver.expected_seq:
                log(f"[RECEPTOR] RECEBIDO seq={seqnum} -> {dados.decode(errors='replace')}")
                ack = criar_pacote(TYPE_ACK, seqnum, b'')
                canal.enviar(sock, ack, addr, is_ack=True)  # ACK via canal (pode ser perdido)
                receiver.expected_seq = 1 - receiver.expected_seq
            else:
                log(f"[RECEPTOR] DUPLICADO seq={seqnum} -> reenviando ACK anterior via canal")
                ack_prev = criar_pacote(TYPE_ACK, 1 - receiver.expected_seq, b'')
                canal.enviar(sock, ack_prev, addr, is_ack=True)

    threading.Thread(target=receiver_worker, daemon=True).start()
    time.sleep(0.5)

    # Sender que usa o canal (DATA via channel, ACK loss simulado pelo receiver usando channel.enviar)
    sender = RDT30Sender(canal, timeout=2.0)
    mensagens = [f"Mensagem {i}".encode() for i in range(20)]

    for m in mensagens:
        sender.enviar(m)

    stats = sender.stats()
    print("\n=== RESULTADOS RDT3.0 ===")
    print(f"Retransmissões: {stats['retransmissions']}")
    print(f"Bytes úteis enviados: {stats['bytes_sent']}")
    print(f"Tempo total (s): {stats['total_time_s']:.3f}")
    print(f"Throughput (B/s): {stats['throughput_Bps']:.2f}")
    print("✓ Teste RDT3.0 finalizado.\n")



if __name__ == "__main__":
    test_rdt21()
