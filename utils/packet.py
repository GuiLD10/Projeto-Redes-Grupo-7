# utils/packet.py
import hashlib
import struct

TYPE_DATA = 0
TYPE_ACK = 1
TYPE_NAK = 2

# Funções da Fase 1A (rdt2.0)
def calcular_checksum(data: bytes) -> bytes:
    return hashlib.md5(data).digest()

def criar_pacote_simples(tipo: int, dados: bytes) -> bytes:
    """Pacote: [tipo(1B)][checksum(16B)][dados]"""
    checksum = calcular_checksum(dados)
    return struct.pack("!B16s", tipo, checksum) + dados

def validar_pacote_simples(pacote: bytes):
    """Retorna (tipo, dados, valido)"""
    tipo, checksum = struct.unpack("!B16s", pacote[:17])
    dados = pacote[17:]
    valido = (checksum == calcular_checksum(dados))
    return tipo, dados, valido

# Funções da Fase 1B (rdt2.1)
def criar_pacote(tipo: int, seqnum: int, dados: bytes) -> bytes:
    """Pacote: [tipo(1B)][seqnum(1B)][checksum(16B)][dados]"""
    checksum = calcular_checksum(dados)
    return struct.pack("!BB16s", tipo, seqnum, checksum) + dados

def validar_pacote(pacote: bytes):
    """Retorna (tipo, seqnum, dados, valido)"""
    tipo, seqnum, checksum = struct.unpack("!BB16s", pacote[:18])
    dados = pacote[18:]
    valido = (checksum == calcular_checksum(dados))
    return tipo, seqnum, dados, valido

# Funções da Fase 2 
def calcular_checksum2(tipo, seqnum, dados: bytes) -> bytes:
    """Calcula checksum MD5 baseado no tipo, seqnum e dados."""
    md5 = hashlib.md5()
    md5.update(struct.pack("!BI", tipo, seqnum))
    md5.update(dados)
    return md5.digest()


def criar_pacote_fase2(tipo: int, seqnum: int, dados: bytes) -> bytes:
    """Cria pacote com cabeçalho [tipo|seqnum|checksum|dados]."""
    checksum = calcular_checksum2(tipo, seqnum, dados)
    # ! = network order, B = tipo (1 byte), I = seqnum (4 bytes), 16s = checksum
    return struct.pack("!BI16s", tipo, seqnum, checksum) + dados


def validar_pacote_fase2(pacote: bytes):
    """Valida o pacote, retorna (tipo, seqnum, dados, valido)."""
    try:
        cabecalho = pacote[:21]
        tipo, seqnum, checksum = struct.unpack("!BI16s", cabecalho)
        dados = pacote[21:]
        valido = checksum == calcular_checksum2(tipo, seqnum, dados)
        return tipo, seqnum, dados, valido
    except Exception:
        return None, None, b"", False
    
class Packet:
    # Tipos de pacote
    DATA = 0
    ACK = 1
    
    def __init__(self, packet_type, seq_num, data=b''):
        self.packet_type = packet_type
        self.seq_num = seq_num
        self.data = data
        self.checksum = self.calculate_checksum()
    
    def calculate_checksum(self):
        """Calcula o checksum do pacote"""
        # Garante que seq_num está no range válido
        seq_num = self.seq_num & 0xFFFFFFFF  # Garante que é um número de 32 bits
        
        header = struct.pack('!BI', self.packet_type, seq_num)
        data_to_hash = header + self.data
        return int(hashlib.md5(data_to_hash).hexdigest()[:8], 16) & 0xFFFFFFFF
    
    def verify_checksum(self):
        """Verifica se o checksum está correto"""
        return self.checksum == self.calculate_checksum()
    
    def to_bytes(self):
        """Converte o pacote para bytes"""
        seq_num = self.seq_num & 0xFFFFFFFF  # Garante que é um número de 32 bits
        header = struct.pack('!BII', self.packet_type, seq_num, self.checksum)
        return header + self.data
    
    @classmethod
    def from_bytes(cls, data):
        """Cria um pacote a partir de bytes"""
        if len(data) < 9:  # Header mínimo (1 + 4 + 4 bytes)
            return None
        
        try:
            header = data[:9]
            packet_data = data[9:]
            packet_type, seq_num, checksum = struct.unpack('!BII', header)
            
            packet = cls(packet_type, seq_num, packet_data)
            packet.checksum = checksum
            
            if not packet.verify_checksum():
                return None
                
            return packet
        except:
            return None
    
    def __str__(self):
        type_str = "DATA" if self.packet_type == self.DATA else "ACK"
        return f"Packet({type_str}, seq={self.seq_num}, data_len={len(self.data)})"