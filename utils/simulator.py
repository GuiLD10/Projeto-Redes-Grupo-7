# utils/simulator.py
import random
import threading
import random
import time
from typing import Callable, Optional

class UnreliableChannel:
    def __init__(self,
                 loss_rate_data=0.0,
                 loss_rate_ack=0.0,
                 corrupt_rate=0.0,
                 delay_range=(0.05, 0.5)):
        """
        loss_rate_data: probabilidade de perda de pacotes DATA
        loss_rate_ack: probabilidade de perda de ACKs
        corrupt_rate: probabilidade de corrupção (aplica tanto a DATA quanto ACKs)
        delay_range: (min_s, max_s) atraso simulado
        """
        self.loss_rate_data = loss_rate_data
        self.loss_rate_ack = loss_rate_ack
        self.corrupt_rate = corrupt_rate
        self.delay_range = delay_range

    def enviar(self, socket_udp, pacote: bytes, destino, is_ack: bool = False):
        """Envia pacote simulando perdas, corrupção e atraso.
           is_ack=True quando o pacote a enviar é um ACK/NAK.
        """
        # escolha de perda conforme tipo
        loss_prob = self.loss_rate_ack if is_ack else self.loss_rate_data
        if random.random() < loss_prob:
            print("[CANAL] Pacote perdido (tipo={})".format("ACK" if is_ack else "DATA"))
            return

        # corrupção
        if random.random() < self.corrupt_rate:
            pacote = self._corromper(pacote)
            print("[CANAL] Pacote corrompido (tipo={})".format("ACK" if is_ack else "DATA"))

        # atraso
        delay = random.uniform(*self.delay_range)
        threading.Timer(delay, lambda: socket_udp.sendto(pacote, destino)).start()

    def _corromper(self, pacote: bytes) -> bytes:
        if not pacote:
            return pacote
        pacote = bytearray(pacote)
        # inverte alguns bytes aleatórios
        num_corruptions = random.randint(1, max(1, min(5, len(pacote))))
        for _ in range(num_corruptions):
            idx = random.randint(0, len(pacote) - 1)
            pacote[idx] ^= 0xFF
        return bytes(pacote)

class NetworkSimulator:
    def __init__(self, loss_probability: float = 0.0, corruption_probability: float = 0.0,
                 delay_range: tuple = (0, 0)):
        """
        Simulador de rede com perdas, corrupção e delay
        
        Args:
            loss_probability: Probabilidade de perda de pacote (0.0 a 1.0)
            corruption_probability: Probabilidade de corromper pacote (0.0 a 1.0)
            delay_range: Tupla (min_delay, max_delay) em segundos
        """
        self.loss_probability = loss_probability
        self.corruption_probability = corruption_probability
        self.delay_range = delay_range
        self.callback = None
        self.packets_sent = 0
        self.packets_lost = 0
        self.packets_corrupted = 0
        
        # Lock para thread safety
        self.lock = threading.Lock()
    
    def set_callback(self, callback: Callable):
        """Define o callback para quando pacotes são recebidos"""
        self.callback = callback
    
    def send(self, data: bytes, is_ack: bool = False):
        """
        Simula o envio de um pacote através da rede
        
        Args:
            data: Dados do pacote
            is_ack: Se é um pacote ACK (para estatísticas)
        """
        with self.lock:
            self.packets_sent += 1
            
            # Verifica se o pacote é perdido
            if random.random() < self.loss_probability:
                self.packets_lost += 1
                print(f"Rede: Pacote perdido! (Total perdidos: {self.packets_lost})")
                return
            
            # Verifica se o pacote é corrompido
            corrupted_data = data
            if random.random() < self.corruption_probability:
                self.packets_corrupted += 1
                # Corrompe alguns bytes
                data_list = bytearray(data)
                if len(data_list) > 10:
                    for _ in range(min(5, len(data_list) // 10)):
                        idx = random.randint(0, len(data_list) - 1)
                        data_list[idx] = random.randint(0, 255)
                    corrupted_data = bytes(data_list)
                print(f"Rede: Pacote corrompido! (Total corrompidos: {self.packets_corrupted})")
            
            # Calcula delay
            delay = random.uniform(self.delay_range[0], self.delay_range[1])
            
            # Agenda a entrega do pacote
            if self.callback:
                if delay > 0:
                    timer = threading.Timer(delay, self._deliver_packet, [corrupted_data])
                    timer.daemon = True
                    timer.start()
                else:
                    self._deliver_packet(corrupted_data)
    
    def _deliver_packet(self, data: bytes):
        """Entrega o pacote com thread safety"""
        with self.lock:
            if self.callback:
                self.callback(data)
    
    def get_statistics(self) -> dict:
        """Retorna estatísticas do simulador"""
        return {
            'packets_sent': self.packets_sent,
            'packets_lost': self.packets_lost,
            'packets_corrupted': self.packets_corrupted,
            'loss_rate': self.packets_lost / max(1, self.packets_sent),
            'corruption_rate': self.packets_corrupted / max(1, self.packets_sent)
        }
    
    def reset_statistics(self):
        """Reseta as estatísticas"""
        self.packets_sent = 0
        self.packets_lost = 0
        self.packets_corrupted = 0