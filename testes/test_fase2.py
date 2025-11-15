import sys
import os
import time
import random

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fase2.gbn import GoBackN
from utils.packet import Packet

class FinalTest:
    def __init__(self):
        self.received_data = []
        
    def sender_deliver_callback(self, data):
        pass
    
    def receiver_deliver_callback(self, data):
        self.received_data.append(data)
    
    def test_efficiency(self):
        """Teste 1: Eficiência - Transferir 1MB de dados"""
        print("=== TESTE 1: EFICIÊNCIA (1MB) ===")
        
        # Configuração
        data_size = 1024 * 1024  # 1MB
        chunk_size = 1024  # 1KB por pacote
        window_size = 5
        
        # Listas para simular rede
        network_data = []
        network_acks = []
        
        # Cria sender e receiver
        sender = GoBackN(
            send_callback=lambda data: network_data.append(data),
            deliver_callback=self.sender_deliver_callback,
            window_size=window_size,
            timeout=0.1
        )
        
        receiver = GoBackN(
            send_callback=lambda data: network_acks.append(data),
            deliver_callback=self.receiver_deliver_callback,
            window_size=window_size
        )
        
        # Prepara dados
        data_to_send = b'X' * data_size
        chunks = [data_to_send[i:i+chunk_size] for i in range(0, len(data_to_send), chunk_size)]
        
        print(f"Transferindo {data_size} bytes em {len(chunks)} pacotes...")
        
        start_time = time.time()
        
        # Envia dados
        chunks_sent = 0
        total_packets_processed = 0
        
        while chunks_sent < len(chunks) or not sender.is_transmission_complete():
            # Envia novos pacotes se houver espaço na janela
            while chunks_sent < len(chunks) and sender.send(chunks[chunks_sent]):
                chunks_sent += 1
            
            # Processa pacotes DATA
            while network_data:
                packet_bytes = network_data.pop(0)
                receiver.receive_packet(packet_bytes)
                total_packets_processed += 1
            
            # Processa pacotes ACK
            while network_acks:
                ack_bytes = network_acks.pop(0)
                sender.receive_packet(ack_bytes)
                total_packets_processed += 1
            
            # Pequena pausa para evitar CPU excessiva
            if not network_data and not network_acks:
                time.sleep(0.001)
        
        end_time = time.time()
        transfer_time = end_time - start_time
        
        # Estatísticas
        stats = sender.get_statistics()
        throughput = data_size / transfer_time / 1024  # KB/s
        utilization = (stats['packets_sent'] - stats['packets_retransmitted']) / max(1, stats['packets_sent'])
        
        print(f"\nRESULTADOS:")
        print(f"Tempo de transferência: {transfer_time:.2f} segundos")
        print(f"Throughput: {throughput:.2f} KB/s")
        print(f"Utilização: {utilization:.2%}")
        print(f"Pacotes enviados: {stats['packets_sent']}")
        print(f"Retransmissões: {stats['packets_retransmitted']}")
        print(f"ACKs recebidos: {stats['acks_received']}")
        print(f"Pacotes recebidos: {len(self.received_data)}")
        print(f"Total de pacotes processados: {total_packets_processed}")
        
        # Verifica integridade
        received_total = sum(len(chunk) for chunk in self.received_data)
        success = received_total == data_size
        print(f"Sucesso: {success} ({received_total} bytes recebidos)")
        
        sender.close()
        receiver.close()
        
        return transfer_time, throughput, utilization
    
    def test_with_losses(self):
        """Teste 2: Com perdas de 10%"""
        print("\n=== TESTE 2: COM PERDAS DE 10% ===")
        
        # Configuração
        data_size = 100 * 1024  # 100KB
        chunk_size = 1024  # 1KB por pacote
        window_size = 5
        
        # Listas para simular rede com perdas
        network_data = []
        network_acks = []
        
        # Cria sender e receiver
        sender = GoBackN(
            send_callback=lambda data: network_data.append(data),
            deliver_callback=self.sender_deliver_callback,
            window_size=window_size,
            timeout=0.2
        )
        
        receiver = GoBackN(
            send_callback=lambda data: network_acks.append(data),
            deliver_callback=self.receiver_deliver_callback,
            window_size=window_size
        )
        
        # Prepara dados
        self.received_data = []
        data_to_send = b'Y' * data_size
        chunks = [data_to_send[i:i+chunk_size] for i in range(0, len(data_to_send), chunk_size)]
        
        print(f"Transferindo {data_size} bytes com 10% de perdas...")
        
        # Envia dados
        chunks_sent = 0
        packets_lost = 0
        
        start_time = time.time()
        
        while chunks_sent < len(chunks) or not sender.is_transmission_complete():
            # Envia novos pacotes
            while chunks_sent < len(chunks) and sender.send(chunks[chunks_sent]):
                chunks_sent += 1
            
            # Processa pacotes DATA com 10% de perdas
            temp_data = list(network_data)
            network_data.clear()
            for packet_bytes in temp_data:
                if random.random() < 0.1:  # 10% de perda
                    packets_lost += 1
                    #print(f"Pacote DATA perdido! Total: {packets_lost}")
                else:
                    receiver.receive_packet(packet_bytes)
            
            # Processa pacotes ACK com 10% de perdas
            temp_acks = list(network_acks)
            network_acks.clear()
            for ack_bytes in temp_acks:
                if random.random() < 0.1:  # 10% de perda
                    packets_lost += 1
                    #print(f"Pacote ACK perdido! Total: {packets_lost}")
                else:
                    sender.receive_packet(ack_bytes)
            
            # Timeout de segurança
            if time.time() - start_time > 30:  # 30 segundos máximo
                print("Timeout: transferência muito lenta")
                break
            
            time.sleep(0.001)
        
        end_time = time.time()
        
        # Estatísticas
        stats = sender.get_statistics()
        received_total = sum(len(chunk) for chunk in self.received_data)
        success = received_total == data_size
        
        print(f"\nRESULTADOS COM PERDAS:")
        print(f"Pacotes enviados: {stats['packets_sent']}")
        print(f"Retransmissões: {stats['packets_retransmitted']}")
        print(f"Pacotes perdidos simulados: {packets_lost}")
        print(f"ACKs recebidos: {stats['acks_received']}")
        print(f"Dados recebidos: {received_total} bytes")
        print(f"Sucesso: {success}")
        print(f"Tempo: {end_time - start_time:.2f} segundos")
        
        sender.close()
        receiver.close()
        
        return success, stats['packets_retransmitted']
    
    def test_performance_analysis(self):
        """Teste 3: Análise de desempenho com diferentes tamanhos de janela"""
        print("\n=== TESTE 3: ANÁLISE DE DESEMPENHO ===")
        
        # Configuração
        data_size = 100 * 1024  # 100KB
        chunk_size = 1024
        window_sizes = [1, 5, 10, 20]
        
        results = []
        
        for window_size in window_sizes:
            print(f"\n--- Testando com janela N={window_size} ---")
            
            # Reset
            self.received_data = []
            network_data = []
            network_acks = []
            
            sender = GoBackN(
                send_callback=lambda data: network_data.append(data),
                deliver_callback=self.sender_deliver_callback,
                window_size=window_size,
                timeout=0.1
            )
            
            receiver = GoBackN(
                send_callback=lambda data: network_acks.append(data),
                deliver_callback=self.receiver_deliver_callback,
                window_size=window_size
            )
            
            # Prepara dados
            data_to_send = b'Z' * data_size
            chunks = [data_to_send[i:i+chunk_size] for i in range(0, len(data_to_send), chunk_size)]
            
            start_time = time.time()
            
            # Envia e processa dados
            chunks_sent = 0
            while chunks_sent < len(chunks) or not sender.is_transmission_complete():
                # Envia
                while chunks_sent < len(chunks) and sender.send(chunks[chunks_sent]):
                    chunks_sent += 1
                
                # Processa DATA
                while network_data:
                    packet_bytes = network_data.pop(0)
                    receiver.receive_packet(packet_bytes)
                
                # Processa ACK
                while network_acks:
                    ack_bytes = network_acks.pop(0)
                    sender.receive_packet(ack_bytes)
                
                if not network_data and not network_acks:
                    time.sleep(0.001)
                
                # Timeout
                if time.time() - start_time > 10:
                    break
            
            end_time = time.time()
            
            # Calcula resultados
            transfer_time = end_time - start_time
            throughput = data_size / transfer_time / 1024 if transfer_time > 0 else 0
            stats = sender.get_statistics()
            
            results.append({
                'window_size': window_size,
                'throughput': throughput,
                'transfer_time': transfer_time,
                'retransmissions': stats['packets_retransmitted'],
                'total_packets': stats['packets_sent']
            })
            
            print(f"Throughput: {throughput:.2f} KB/s")
            print(f"Retransmissões: {stats['packets_retransmitted']}")
            
            sender.close()
            receiver.close()
        
        # Exibe resultados
        print("\n=== RESUMO FINAL ===")
        for result in results:
            print(f"N={result['window_size']}: {result['throughput']:.2f} KB/s, "
                  f"{result['retransmissions']} retransmissões")
        
        return results

def main():
    test = FinalTest()
    
    # Executa todos os testes
    test.test_efficiency()
    test.test_with_losses()
    test.test_performance_analysis()
    
    print("\n🎉 TODOS OS TESTES CONCLUÍDOS!")

if __name__ == "__main__":
    main()