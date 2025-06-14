import os
import sys
import psycopg2
import psycopg2.extras
import time
import asyncio
import concurrent.futures
from datetime import datetime, timedelta
from flask import Flask, jsonify, request
from flask_cors import CORS
import json
import threading
import schedule
from collections import defaultdict
import hashlib

# DON'T CHANGE THIS !!!
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

app = Flask(__name__)
CORS(app, origins="*")

# Configurações
app.config['SECRET_KEY'] = 'real_data_service_v2_secret_2025'
app.config['MAX_WORKERS'] = 4  # Para processamento paralelo

# Configurações do banco externo (operação)
EXTERNAL_DB_CONFIG = {
    'host': '177.115.223.216',
    'port': 5999,
    'database': 'dados_interno',
    'user': 'userschaphz',
    'password': 'mschaphz8881!',
    'connect_timeout': 30
}

# Cache avançado com particionamento
data_cache = {
    'users': defaultdict(list),
    'transactions': defaultdict(list),
    'affiliates': defaultdict(list),
    'commissions': defaultdict(list),
    'metadata': {
        'last_sync': None,
        'sync_status': 'never_synced',
        'total_records': 0,
        'partitions': 0,
        'sync_duration': 0
    }
}

# Pool de conexões
connection_pool = []
pool_lock = threading.Lock()

# Lock para thread safety
cache_lock = threading.Lock()

def create_connection_pool(size=5):
    """Cria pool de conexões para melhor performance"""
    global connection_pool
    with pool_lock:
        for _ in range(size):
            try:
                conn = psycopg2.connect(**EXTERNAL_DB_CONFIG)
                connection_pool.append(conn)
            except Exception as e:
                print(f"Erro ao criar conexão no pool: {e}")

def get_pooled_connection():
    """Obtém conexão do pool"""
    with pool_lock:
        if connection_pool:
            return connection_pool.pop()
    
    # Se não há conexões no pool, cria uma nova
    try:
        return psycopg2.connect(**EXTERNAL_DB_CONFIG)
    except Exception as e:
        print(f"Erro ao criar nova conexão: {e}")
        return None

def return_connection(conn):
    """Retorna conexão para o pool"""
    if conn and not conn.closed:
        with pool_lock:
            if len(connection_pool) < 10:  # Limite do pool
                connection_pool.append(conn)
            else:
                conn.close()

def fetch_data_partition(table_name, offset, limit):
    """Busca uma partição de dados de forma paralela"""
    conn = get_pooled_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Queries otimizadas com paginação
        queries = {
            'users': f"""
                SELECT id, nome, email, telefone, status, data_cadastro, tipo_usuario,
                       EXTRACT(EPOCH FROM data_cadastro) as timestamp_cadastro
                FROM usuarios 
                WHERE ativo = true 
                ORDER BY id
                OFFSET {offset} LIMIT {limit}
            """,
            'transactions': f"""
                SELECT id, usuario_id, valor, tipo_transacao, status, data_transacao, descricao,
                       EXTRACT(EPOCH FROM data_transacao) as timestamp_transacao
                FROM transacoes 
                WHERE data_transacao >= NOW() - INTERVAL '60 days'
                ORDER BY id
                OFFSET {offset} LIMIT {limit}
            """,
            'affiliates': f"""
                SELECT id, usuario_id, codigo_afiliado, nivel, comissao_percentual, status, data_ativacao,
                       EXTRACT(EPOCH FROM data_ativacao) as timestamp_ativacao
                FROM afiliados 
                WHERE status = 'ativo'
                ORDER BY id
                OFFSET {offset} LIMIT {limit}
            """,
            'commissions': f"""
                SELECT id, afiliado_id, transacao_id, valor_comissao, percentual, status, data_calculo,
                       EXTRACT(EPOCH FROM data_calculo) as timestamp_calculo
                FROM comissoes 
                WHERE data_calculo >= NOW() - INTERVAL '30 days'
                ORDER BY id
                OFFSET {offset} LIMIT {limit}
            """
        }
        
        if table_name in queries:
            cursor.execute(queries[table_name])
            results = cursor.fetchall()
            return [dict(row) for row in results]
        else:
            return []
            
    except Exception as e:
        print(f"Erro ao buscar partição {offset}-{offset+limit} da tabela {table_name}: {e}")
        return []
    finally:
        return_connection(conn)

def sync_table_parallel(table_name, max_workers=4):
    """Sincroniza uma tabela usando processamento paralelo"""
    partition_size = 1000
    all_data = []
    
    # Primeiro, descobrir o total de registros
    conn = get_pooled_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        
        count_queries = {
            'users': "SELECT COUNT(*) FROM usuarios WHERE ativo = true",
            'transactions': "SELECT COUNT(*) FROM transacoes WHERE data_transacao >= NOW() - INTERVAL '60 days'",
            'affiliates': "SELECT COUNT(*) FROM afiliados WHERE status = 'ativo'",
            'commissions': "SELECT COUNT(*) FROM comissoes WHERE data_calculo >= NOW() - INTERVAL '30 days'"
        }
        
        if table_name in count_queries:
            cursor.execute(count_queries[table_name])
            total_records = cursor.fetchone()[0]
        else:
            return []
            
    except Exception as e:
        print(f"Erro ao contar registros de {table_name}: {e}")
        return []
    finally:
        return_connection(conn)
    
    if total_records == 0:
        return []
    
    # Calcular partições
    partitions = [(i, min(partition_size, total_records - i)) 
                  for i in range(0, total_records, partition_size)]
    
    print(f"Sincronizando {table_name}: {total_records} registros em {len(partitions)} partições")
    
    # Processar partições em paralelo
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(fetch_data_partition, table_name, offset, limit)
            for offset, limit in partitions
        ]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                partition_data = future.result()
                all_data.extend(partition_data)
            except Exception as e:
                print(f"Erro ao processar partição de {table_name}: {e}")
    
    return all_data

def sync_all_data_v2():
    """Sincronização otimizada com processamento paralelo"""
    start_time = time.time()
    
    with cache_lock:
        try:
            data_cache['metadata']['sync_status'] = 'syncing'
            
            # Sincronizar cada tipo de dado em paralelo
            tables = ['users', 'transactions', 'affiliates', 'commissions']
            total_records = 0
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                # Executar sincronização de tabelas em paralelo
                futures = {
                    executor.submit(sync_table_parallel, table): table 
                    for table in tables
                }
                
                for future in concurrent.futures.as_completed(futures):
                    table = futures[future]
                    try:
                        data = future.result()
                        
                        # Particionar dados por hash para distribuição
                        partitioned_data = defaultdict(list)
                        for record in data:
                            partition_key = hash(str(record.get('id', 0))) % 4
                            partitioned_data[partition_key].append(record)
                        
                        data_cache[table] = partitioned_data
                        total_records += len(data)
                        
                        print(f"Sincronizados {len(data)} registros de {table} em {len(partitioned_data)} partições")
                        
                    except Exception as e:
                        print(f"Erro ao sincronizar {table}: {e}")
            
            # Atualizar metadata
            sync_duration = time.time() - start_time
            data_cache['metadata'].update({
                'last_sync': datetime.now(),
                'sync_status': 'success',
                'total_records': total_records,
                'partitions': 4,
                'sync_duration': sync_duration
            })
            
            print(f"Sincronização v2 completa: {total_records} registros em {sync_duration:.2f}s")
            
        except Exception as e:
            data_cache['metadata']['sync_status'] = 'error'
            print(f"Erro na sincronização v2: {e}")

def background_sync_v2():
    """Executa sincronização otimizada em background"""
    schedule.every(10).minutes.do(sync_all_data_v2)
    
    while True:
        schedule.run_pending()
        time.sleep(30)  # Check mais frequente

# Inicializar pool de conexões
create_connection_pool()

# Iniciar thread de sincronização em background
sync_thread = threading.Thread(target=background_sync_v2, daemon=True)
sync_thread.start()

@app.route('/health', methods=['GET'])
def health_check():
    """Health check avançado"""
    # Testar pool de conexões
    conn = get_pooled_connection()
    db_status = 'connected' if conn else 'disconnected'
    pool_size = len(connection_pool)
    
    if conn:
        return_connection(conn)
    
    return jsonify({
        'status': 'healthy',
        'service': 'real-data-service-v2',
        'version': '2.0',
        'timestamp': datetime.now().isoformat(),
        'external_db_status': db_status,
        'connection_pool_size': pool_size,
        'last_sync': data_cache['metadata']['last_sync'].isoformat() if data_cache['metadata']['last_sync'] else None,
        'sync_status': data_cache['metadata']['sync_status'],
        'sync_duration': data_cache['metadata']['sync_duration'],
        'total_cached_records': data_cache['metadata']['total_records'],
        'partitions': data_cache['metadata']['partitions']
    }), 200

@app.route('/sync/v2', methods=['POST'])
def manual_sync_v2():
    """Sincronização manual otimizada"""
    threading.Thread(target=sync_all_data_v2, daemon=True).start()
    return jsonify({
        'message': 'Sincronização v2 iniciada',
        'timestamp': datetime.now().isoformat(),
        'status': data_cache['metadata']['sync_status']
    })

@app.route('/data/v2/users', methods=['GET'])
def get_users_v2():
    """Retorna dados de usuários com busca otimizada"""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    partition = request.args.get('partition', None, type=int)
    
    with cache_lock:
        if partition is not None and partition in data_cache['users']:
            # Busca em partição específica
            users_data = data_cache['users'][partition]
        else:
            # Busca em todas as partições
            users_data = []
            for partition_data in data_cache['users'].values():
                users_data.extend(partition_data)
    
    # Paginação
    start = (page - 1) * per_page
    end = start + per_page
    users = users_data[start:end]
    total = len(users_data)
    
    return jsonify({
        'users': users,
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total': total,
            'pages': (total + per_page - 1) // per_page
        },
        'metadata': {
            'last_sync': data_cache['metadata']['last_sync'].isoformat() if data_cache['metadata']['last_sync'] else None,
            'partitions_available': list(data_cache['users'].keys()),
            'version': '2.0'
        }
    })

@app.route('/data/v2/transactions', methods=['GET'])
def get_transactions_v2():
    """Retorna dados de transações com busca otimizada"""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    partition = request.args.get('partition', None, type=int)
    
    with cache_lock:
        if partition is not None and partition in data_cache['transactions']:
            transactions_data = data_cache['transactions'][partition]
        else:
            transactions_data = []
            for partition_data in data_cache['transactions'].values():
                transactions_data.extend(partition_data)
    
    start = (page - 1) * per_page
    end = start + per_page
    transactions = transactions_data[start:end]
    total = len(transactions_data)
    
    return jsonify({
        'transactions': transactions,
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total': total,
            'pages': (total + per_page - 1) // per_page
        },
        'metadata': {
            'last_sync': data_cache['metadata']['last_sync'].isoformat() if data_cache['metadata']['last_sync'] else None,
            'partitions_available': list(data_cache['transactions'].keys()),
            'version': '2.0'
        }
    })

@app.route('/data/v2/stats', methods=['GET'])
def get_stats_v2():
    """Estatísticas avançadas dos dados"""
    with cache_lock:
        stats = {
            'version': '2.0',
            'total_records': data_cache['metadata']['total_records'],
            'partitions': data_cache['metadata']['partitions'],
            'last_sync': data_cache['metadata']['last_sync'].isoformat() if data_cache['metadata']['last_sync'] else None,
            'sync_status': data_cache['metadata']['sync_status'],
            'sync_duration': data_cache['metadata']['sync_duration'],
            'records_by_table': {},
            'records_by_partition': {}
        }
        
        # Contar registros por tabela e partição
        for table in ['users', 'transactions', 'affiliates', 'commissions']:
            table_total = sum(len(partition) for partition in data_cache[table].values())
            stats['records_by_table'][table] = table_total
            
            partition_counts = {
                str(partition_id): len(partition_data)
                for partition_id, partition_data in data_cache[table].items()
            }
            stats['records_by_partition'][table] = partition_counts
    
    return jsonify(stats)

@app.route('/performance/test', methods=['GET'])
def performance_test():
    """Teste de performance do serviço v2"""
    start_time = time.time()
    
    # Simular várias operações
    operations = []
    
    # Teste de busca em partições
    for partition in range(4):
        partition_start = time.time()
        with cache_lock:
            users_count = len(data_cache['users'].get(partition, []))
            transactions_count = len(data_cache['transactions'].get(partition, []))
        partition_time = time.time() - partition_start
        
        operations.append({
            'operation': f'partition_{partition}_query',
            'duration': partition_time,
            'users_count': users_count,
            'transactions_count': transactions_count
        })
    
    total_time = time.time() - start_time
    
    return jsonify({
        'performance_test': {
            'total_duration': total_time,
            'operations': operations,
            'avg_operation_time': total_time / len(operations) if operations else 0,
            'timestamp': datetime.now().isoformat()
        }
    })

# Executar sincronização inicial ao iniciar o serviço
@app.before_first_request
def initial_sync_v2():
    """Executa sincronização inicial otimizada"""
    threading.Thread(target=sync_all_data_v2, daemon=True).start()

if __name__ == '__main__':
    # Executar sincronização inicial
    print("Iniciando sincronização inicial v2...")
    threading.Thread(target=sync_all_data_v2, daemon=True).start()
    
    app.run(host='0.0.0.0', port=3000, debug=True)

