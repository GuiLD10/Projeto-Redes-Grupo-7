import time
import threading
from typing import Callable, Optional
from utils.packet import Packet

class GoBackN:
    def __init__(self, send_callback: Callable, deliver_callback: Callable, 
                 window_size: int = 5, timeout: float = 1.0):
        """
        Implementação do protocolo Go-Back-N
        
        Args:
            send_callback: Função para enviar pacotes pela rede
            deliver_callback: Função para entregar dados à aplicação
            window_size: Tamanho da janela (N)
            timeout: Timeout para retransmissão (segundos)
        """
        # Callbacks
        self.send_callback = send_callback
        self.deliver_callback = deliver_callback
        
        # Parâmetros do protocolo
        self.window_size = window_size
        self.timeout = timeout
        
        # Variáveis do remetente
        self.base = 0  # Primeiro pacote não confirmado
        self.next_seq_num = 0  # Próximo número de sequência a ser usado
        self.sender_buffer = {}  # Buffer de pacotes enviados
        self.timer = None  # Timer para o pacote mais antigo
        self.timer_running = False
        
        # Variáveis do receptor
        self.expected_seq_num = 0  # Próximo número de sequência esperado
        
        # Estatísticas
        self.packets_sent = 0
        self.packets_retransmitted = 0
        self.acks_received = 0
        
        # Lock para thread safety
        self.lock = threading.Lock()
        
        # Flag para controle de execução
        self.running = True
    
    def is_transmission_complete(self) -> bool:
        """Verifica se a transmissão está completa"""
        with self.lock:
            return self.base >= self.next_seq_num and self.next_seq_num > 0

    # ===== MÉTODOS DO REMETENTE =====
    
    def send(self, data: bytes) -> bool:
        """
        Envia dados através do protocolo GBN
        
        Args:
            data: Dados a serem enviados
            
        Returns:
            True se os dados foram aceitos para envio
        """
        with self.lock:
            # Verifica se há espaço na janela
            if self.next_seq_num >= self.base + self.window_size:
                return False  # Janela cheia
            
            # Cria e armazena o pacote
            packet = Packet(Packet.DATA, self.next_seq_num, data)
            self.sender_buffer[self.next_seq_num] = packet
            
            # Envia o pacote
            self._send_packet(packet)
            self.packets_sent += 1
            
            print(f"Sender: Enviando pacote {self.next_seq_num}, base={self.base}")
            
            # Inicia o timer se for o primeiro pacote não confirmado
            if self.base == self.next_seq_num:
                self._start_timer()
            
            self.next_seq_num += 1
            return True
    
    def _send_packet(self, packet: Packet):
        """Envia um pacote através do callback"""
        self.send_callback(packet.to_bytes())
    
    def _start_timer(self):
        """Inicia o timer para o pacote mais antigo"""
        if self.timer_running:
            self.timer.cancel()
        
        self.timer = threading.Timer(self.timeout, self._timeout_handler)
        self.timer.daemon = True
        self.timer.start()
        self.timer_running = True
        print(f"Sender: Timer iniciado para base={self.base}")
    
    def _stop_timer(self):
        """Para o timer"""
        if self.timer_running:
            self.timer.cancel()
            self.timer_running = False
            print(f"Sender: Timer parado")
    
    def _timeout_handler(self):
        """Handler para timeout - retransmite todos os pacotes na janela"""
        with self.lock:
            if not self.running:
                return
                
            print(f"Sender: Timeout! Retransmitindo pacotes de {self.base} a {self.next_seq_num - 1}")
            
            # Retransmite todos os pacotes na janela
            for seq_num in range(self.base, self.next_seq_num):
                if seq_num in self.sender_buffer:
                    self._send_packet(self.sender_buffer[seq_num])
                    self.packets_retransmitted += 1
            
            # Reinicia o timer
            self._start_timer()
    
    def handle_ack(self, ack_packet: Packet):
        """
        Processa um pacote ACK recebido
        
        Args:
            ack_packet: Pacote ACK recebido
        """
        with self.lock:
            if not self.running:
                return
            
            ack_num = ack_packet.seq_num
            self.acks_received += 1
            
            print(f"Sender: ACK {ack_num} recebido, base atual: {self.base}")
            
            # ACK cumulativo: move base para ack_num + 1
            # No GBN, ACK(n) confirma todos os pacotes até n
            if ack_num >= self.base - 1:  # Permite ACKs um pouco atrasados
                old_base = self.base
                self.base = ack_num + 1
                
                # Remove pacotes confirmados do buffer
                for seq_num in range(old_base, self.base):
                    if seq_num in self.sender_buffer:
                        del self.sender_buffer[seq_num]
                
                print(f"Sender: Base movida de {old_base} para {self.base}")
                
                # Gerencia o timer
                if self.base == self.next_seq_num:
                    # Todos os pacotes foram confirmados
                    self._stop_timer()
                else:
                    # Reinicia o timer para o novo pacote mais antigo
                    self._start_timer()
    
    # ===== MÉTODOS DO RECEPTOR =====
    
    def handle_data(self, data_packet: Packet):
        """
        Processa um pacote de dados recebido
        
        Args:
            data_packet: Pacote de dados recebido
        """
        with self.lock:
            if not self.running:
                return
            
            seq_num = data_packet.seq_num
            
            print(f"Receiver: Pacote {seq_num} recebido, esperado: {self.expected_seq_num}")
            
            # Pacote com número de sequência esperado
            if seq_num == self.expected_seq_num:
                print(f"Receiver: Pacote {seq_num} recebido em ordem, entregando à aplicação")
                
                # Entrega os dados à aplicação
                try:
                    self.deliver_callback(data_packet.data)
                except Exception as e:
                    print(f"Erro no deliver_callback: {e}")
                
                # Envia ACK e incrementa número esperado
                ack_packet = Packet(Packet.ACK, self.expected_seq_num)
                self._send_packet(ack_packet)
                print(f"Receiver: Enviando ACK {self.expected_seq_num}")
                self.expected_seq_num += 1
                
            else:
                # Pacote fora de ordem - descarta e reenvia ACK do último pacote entregue em ordem
                print(f"Receiver: Pacote {seq_num} recebido fora de ordem. Descartando.")
                
                # Envia ACK para o último pacote entregue em ordem
                # Se expected_seq_num é 0, significa que nenhum pacote foi recebido ainda
                # Nesse caso, enviamos ACK para -1 (ou 0 dependendo da implementação)
                ack_num = max(0, self.expected_seq_num - 1)  # Garante que não seja negativo
                ack_packet = Packet(Packet.ACK, ack_num)
                self._send_packet(ack_packet)
                print(f"Receiver: Reenviando ACK {ack_num}")
    # ===== MÉTODOS GERAIS =====
    
    def receive_packet(self, packet_bytes: bytes):
        """
        Processa um pacote recebido da rede
        
        Args:
            packet_bytes: Bytes do pacote recebido
        """
        packet = Packet.from_bytes(packet_bytes)
        if packet is None:
            print("Pacote corrompido recebido")
            return
        
        if packet.packet_type == Packet.DATA:
            self.handle_data(packet)
        elif packet.packet_type == Packet.ACK:
            self.handle_ack(packet)
    
    def get_statistics(self) -> dict:
        """Retorna estatísticas do protocolo"""
        with self.lock:
            return {
                'packets_sent': self.packets_sent,
                'packets_retransmitted': self.packets_retransmitted,
                'acks_received': self.acks_received,
                'window_size': self.window_size,
                'current_base': self.base,
                'next_seq_num': self.next_seq_num,
                'expected_seq_num': self.expected_seq_num
            }
    
    def close(self):
        """Encerra o protocolo"""
        with self.lock:
            self.running = False
            self._stop_timer()