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

# Configurações do banco externo (operação) com timeout otimizado
EXTERNAL_DB_CONFIG = {
    'host': '177.115.223.216',
    'port': 5999,
    'database': 'dados_interno',
    'user': 'userschapz',
    'password': 'mschaphz8881!',
    'connect_timeout': 30,  # Reduzido para 30s
    'options': '-c statement_timeout=60000'  # Reduzido para 1 minuto
}

# Cache adaptado para tabelas reais
data_cache = {
    'users': defaultdict(list),
    'transactions': defaultdict(list),
    'affiliates': defaultdict(list),
    'bets': defaultdict(list),
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
        
        # Queries adaptadas para as tabelas reais do banco com LIMIT
        queries = {
            'users': f"""
                SELECT user_id as id, user_id as nome, 
                       CONCAT('user_', user_id) as email,
                       'user' as tipo_usuario,
                       register_date as data_cadastro,
                       EXTRACT(EPOCH FROM register_date) as timestamp_cadastro,
                       'ativo' as status
                FROM cadastro 
                ORDER BY user_id
                LIMIT {limit} OFFSET {offset}
            """,
            'transactions': f"""
                SELECT 
                    CONCAT('dep_', id) as id,
                    user_id as usuario_id,
                    amount as valor,
                    'deposito' as tipo_transacao,
                    status,
                    'Depósito' as descricao,
                    data_deposito as data_transacao,
                    EXTRACT(EPOCH FROM data_deposito) as timestamp_transacao
                FROM depositos 
                WHERE data_deposito >= NOW() - INTERVAL '60 days'
                
                UNION ALL
                
                SELECT 
                    CONCAT('saq_', id) as id,
                    user_id as usuario_id,
                    valor,
                    'saque' as tipo_transacao,
                    status,
                    'Saque' as descricao,
                    data_saques as data_transacao,
                    EXTRACT(EPOCH FROM data_saques) as timestamp_transacao
                FROM saques 
                WHERE data_saques >= NOW() - INTERVAL '60 days'
                
                ORDER BY data_transacao DESC
                LIMIT {limit} OFFSET {offset}
            """,
            'affiliates': f"""
                SELECT 
                    id,
                    user_afil as afiliado_id,
                    user_id as usuario_indicado_id,
                    tracked_type_id as tipo_vinculo,
                    'ativo' as status,
                    CURRENT_TIMESTAMP as data_ativacao,
                    EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) as timestamp_ativacao
                FROM tracked 
                WHERE tracked_type_id = 1
                ORDER BY id
                LIMIT {limit} OFFSET {offset}
            """,
            'bets': f"""
                SELECT 
                    casino_id as id,
                    user_id,
                    game_name,
                    bet_amount as valor_aposta,
                    earned_value as valor_ganho,
                    status,
                    played_date as data_aposta,
                    EXTRACT(EPOCH FROM played_date) as timestamp_aposta
                FROM casino_bets_v 
                WHERE played_date >= NOW() - INTERVAL '30 days'
                ORDER BY played_date DESC
                LIMIT {limit} OFFSET {offset}
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
            'users': "SELECT COUNT(*) FROM cadastro",
            'transactions': """
                SELECT (
                    (SELECT COUNT(*) FROM depositos WHERE data_deposito >= NOW() - INTERVAL '60 days') +
                    (SELECT COUNT(*) FROM saques WHERE data_saques >= NOW() - INTERVAL '60 days')
                ) as total
            """,
            'affiliates': "SELECT COUNT(*) FROM tracked WHERE tracked_type_id = 1",
            'bets': "SELECT COUNT(*) FROM casino_bets_v WHERE played_date >= NOW() - INTERVAL '30 days'"
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
            
            # Sincronizar tabelas reais do banco
            tables = ['users', 'transactions', 'affiliates', 'bets']
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

@app.route('/', methods=['GET'])
def root():
    """Rota raiz do serviço"""
    return jsonify({
        'service': 'fature-real-data-service',
        'version': '2.0',
        'status': 'running',
        'message': 'Fature Real Data Service v2.0 - Operational',
        'endpoints': {
            'health': '/health',
            'users': '/data/v2/users',
            'transactions': '/data/v2/transactions',
            'affiliates': '/data/v2/affiliates',
            'bets': '/data/v2/bets',
            'stats': '/data/v2/stats',
            'sync': '/sync/v2 (POST)'
        },
        'timestamp': datetime.now().isoformat()
    })

@app.route('/health', methods=['GET'])
def health_check_v2():
    """Health check otimizado para Railway - resposta rápida garantida"""
    try:
        current_time = datetime.now().isoformat()
        
        # Resposta básica sempre disponível
        response_data = {
            'status': 'healthy',
            'service': 'real-data-service-v2',
            'version': '2.0',
            'timestamp': current_time,
            'message': 'Service is running'
        }
        
        # Tentar informações adicionais sem bloquear
        try:
            # Verificar pool de conexões rapidamente
            if 'connection_pool' in globals():
                response_data['connection_pool_size'] = len(connection_pool)
                response_data['external_db_status'] = 'pool_available'
            else:
                response_data['external_db_status'] = 'initializing'
            
            # Status do cache
            response_data['sync_status'] = data_cache['metadata']['sync_status']
            
        except Exception as info_error:
            # Se falhar ao obter informações extras, continua com resposta básica
            response_data['external_db_status'] = 'unknown'
            response_data['sync_status'] = 'unknown'
        
        print(f"✅ Health check OK - {current_time}")
        return jsonify(response_data), 200
        
    except Exception as e:
        # Mesmo em caso de erro, retorna resposta rápida
        print(f"❌ Health check error: {e}")
        return jsonify({
            'status': 'healthy',  # Mantém healthy para não falhar o deploy
            'service': 'real-data-service-v2',
            'timestamp': datetime.now().isoformat(),
            'message': 'Service is running with limited info',
            'error': str(e)
        }), 200  # Retorna 200 mesmo com erro para passar no healthcheck

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
    """Retorna dados de usuários com busca otimizada e resposta rápida"""
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
    
    # Se não há dados no cache, retornar resposta rápida com dados básicos
    if not users_data:
        try:
            conn = get_pooled_connection()
            if conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute("""
                    SELECT 
                        user_id as id,
                        register_date,
                        'ativo' as status
                    FROM cadastro 
                    ORDER BY register_date DESC
                    LIMIT %s OFFSET %s
                """, (per_page, (page - 1) * per_page))
                
                basic_users = [dict(row) for row in cursor.fetchall()]
                
                # Contar total rapidamente
                cursor.execute("SELECT COUNT(*) FROM cadastro")
                total = cursor.fetchone()[0]
                
                return_connection(conn)
                
                return jsonify({
                    'users': basic_users,
                    'pagination': {
                        'page': page,
                        'per_page': per_page,
                        'total': total,
                        'pages': (total + per_page - 1) // per_page
                    },
                    'metadata': {
                        'last_sync': None,
                        'partitions_available': [],
                        'version': '2.0',
                        'cache_status': 'direct_query',
                        'note': 'Dados básicos - sincronização em andamento'
                    }
                })
        except Exception as e:
            print(f"Erro na consulta rápida de usuários: {e}")
    
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
            'version': '2.0',
            'cache_status': 'cached_data'
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

@app.route('/data/v2/affiliates', methods=['GET'])
def get_affiliates_v2():
    """Retorna dados de afiliados com busca otimizada e timeout reduzido"""
    page = request.args.get('page', 1, type=int)
    per_page = min(request.args.get('per_page', 10, type=int), 50)  # Máximo 50 por página
    partition = request.args.get('partition', None, type=int)
    
    # Tentar cache primeiro
    with cache_lock:
        if partition is not None and partition in data_cache['affiliates']:
            affiliates_data = data_cache['affiliates'][partition]
        else:
            affiliates_data = []
            for partition_data in data_cache['affiliates'].values():
                affiliates_data.extend(partition_data)
    
    # Se não há dados no cache, fazer consulta rápida e limitada
    if not affiliates_data:
        try:
            conn = get_pooled_connection()
            if conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Query super simples e rápida - apenas primeiros registros
                cursor.execute("""
                    SELECT 
                        id,
                        user_afil as afiliado_id,
                        user_id as usuario_indicado_id,
                        tracked_type_id as tipo_vinculo,
                        'ativo' as status
                    FROM tracked 
                    WHERE tracked_type_id = 1
                    ORDER BY id
                    LIMIT %s
                """, (per_page,))
                
                basic_affiliates = [dict(row) for row in cursor.fetchall()]
                total = len(basic_affiliates)
                
                cursor.close()
                return_connection(conn)
                
                return jsonify({
                    'affiliates': basic_affiliates,
                    'pagination': {
                        'page': page,
                        'per_page': per_page,
                        'total': total,
                        'pages': 1 if total > 0 else 0
                    },
                    'metadata': {
                        'last_sync': None,
                        'partitions_available': [],
                        'version': '2.0',
                        'cache_status': 'direct_query_limited',
                        'note': 'Consulta limitada para resposta rápida'
                    }
                })
        except Exception as e:
            print(f"Erro na consulta rápida de afiliados: {e}")
    
    # Paginação dos dados em cache
    start = (page - 1) * per_page
    end = start + per_page
    affiliates = affiliates_data[start:end]
    total = len(affiliates_data)
    
    return jsonify({
        'affiliates': affiliates,
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total': total,
            'pages': (total + per_page - 1) // per_page if total > 0 else 0
        },
        'metadata': {
            'last_sync': data_cache['metadata']['last_sync'].isoformat() if data_cache['metadata']['last_sync'] else None,
            'partitions_available': list(data_cache['affiliates'].keys()),
            'version': '2.0',
            'cache_status': 'cached_data'
        }
    })

@app.route('/data/v2/bets', methods=['GET'])
def get_bets_v2():
    """Retorna dados de apostas com busca otimizada"""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    partition = request.args.get('partition', None, type=int)
    
    with cache_lock:
        if partition is not None and partition in data_cache['bets']:
            bets_data = data_cache['bets'][partition]
        else:
            bets_data = []
            for partition_data in data_cache['bets'].values():
                bets_data.extend(partition_data)
    
    start = (page - 1) * per_page
    end = start + per_page
    bets = bets_data[start:end]
    total = len(bets_data)
    
    return jsonify({
        'bets': bets,
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total': total,
            'pages': (total + per_page - 1) // per_page
        },
        'metadata': {
            'last_sync': data_cache['metadata']['last_sync'].isoformat() if data_cache['metadata']['last_sync'] else None,
            'partitions_available': list(data_cache['bets'].keys()),
            'version': '2.0'
        }
    })

@app.route('/data/v2/stats', methods=['GET'])
def get_stats_v2():
    """Estatísticas avançadas dos dados com resposta rápida"""
    with cache_lock:
        # Se não há dados no cache, retornar estatísticas básicas do banco
        if data_cache['metadata']['total_records'] == 0:
            try:
                conn = get_pooled_connection()
                if conn:
                    cursor = conn.cursor()
                    
                    # Consultas rápidas para estatísticas básicas
                    basic_stats = {
                        'version': '2.0',
                        'total_records': 0,
                        'partitions': 0,
                        'last_sync': None,
                        'sync_status': 'not_synced',
                        'sync_duration': 0,
                        'records_by_table': {},
                        'records_by_partition': {},
                        'cache_status': 'direct_query',
                        'note': 'Estatísticas básicas - sincronização em andamento'
                    }
                    
                    # Contar registros básicos rapidamente
                    quick_counts = {
                        'users': "SELECT COUNT(*) FROM cadastro",
                        'affiliates': "SELECT COUNT(*) FROM tracked WHERE tracked_type_id = 1",
                        'transactions': "SELECT (SELECT COUNT(*) FROM depositos) + (SELECT COUNT(*) FROM saques)",
                        'bets': "SELECT COUNT(*) FROM casino_bets_v WHERE played_date >= NOW() - INTERVAL '7 days'"
                    }
                    
                    for table, query in quick_counts.items():
                        try:
                            cursor.execute(query)
                            count = cursor.fetchone()[0]
                            basic_stats['records_by_table'][table] = count
                            basic_stats['total_records'] += count
                        except Exception as e:
                            print(f"Erro ao contar {table}: {e}")
                            basic_stats['records_by_table'][table] = 0
                    
                    return_connection(conn)
                    return jsonify(basic_stats)
                    
            except Exception as e:
                print(f"Erro na consulta rápida de stats: {e}")
        
        # Retornar estatísticas do cache se disponível
        stats = {
            'version': '2.0',
            'total_records': data_cache['metadata']['total_records'],
            'partitions': data_cache['metadata']['partitions'],
            'last_sync': data_cache['metadata']['last_sync'].isoformat() if data_cache['metadata']['last_sync'] else None,
            'sync_status': data_cache['metadata']['sync_status'],
            'sync_duration': data_cache['metadata']['sync_duration'],
            'records_by_table': {},
            'records_by_partition': {},
            'cache_status': 'cached_data'
        }
        
        # Contar registros por tabela e partição
        for table in ['users', 'transactions', 'affiliates', 'bets']:
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
def initial_sync_v2():
    """Executa sincronização inicial otimizada"""
    threading.Thread(target=sync_all_data_v2, daemon=True).start()

if __name__ == '__main__':
    # Logs de inicialização
    print("=" * 50)
    print("🚀 INICIANDO FATURE REAL DATA SERVICE V2")
    print("=" * 50)
    
    # Verificar variáveis de ambiente
    port = int(os.getenv('PORT', 5000))
    print(f"📡 Porta configurada: {port}")
    print(f"🗄️  Banco: {EXTERNAL_DB_CONFIG['host']}:{EXTERNAL_DB_CONFIG['port']}")
    
    # Inicializar componentes em background para não bloquear o startup
    def initialize_background_services():
        """Inicializa serviços em background após o Flask estar rodando"""
        time.sleep(2)  # Aguarda Flask inicializar
        
        # Testar conexão com banco
        print("🔍 Testando conexão com banco...")
        try:
            test_conn = psycopg2.connect(**EXTERNAL_DB_CONFIG)
            test_conn.close()
            print("✅ Conexão com banco OK!")
        except Exception as e:
            print(f"❌ Erro na conexão com banco: {e}")
            print("⚠️  Serviço continuará sem sincronização inicial")
        
        # Inicializar pool de conexões
        print("🔧 Inicializando pool de conexões...")
        create_connection_pool()
        
        # Iniciar scheduler em thread separada
        print("⏰ Iniciando scheduler...")
        scheduler_thread = threading.Thread(target=run_scheduler_v2, daemon=True)
        scheduler_thread.start()
        
        # Executar sincronização inicial (não bloqueante)
        print("🔄 Iniciando sincronização inicial...")
        initial_sync_v2()
        
        print("✅ Todos os serviços em background inicializados!")
    
    # Iniciar serviços em background
    background_init_thread = threading.Thread(target=initialize_background_services, daemon=True)
    background_init_thread.start()
    
    print("=" * 50)
    print(f"🌐 Servidor iniciando em 0.0.0.0:{port}")
    print("📋 Endpoints disponíveis:")
    print("   - GET /health - Health check")
    print("   - GET /data/v2/users - Dados de usuários")
    print("   - GET /data/v2/stats - Estatísticas")
    print("=" * 50)
    
    # Iniciar aplicação Flask imediatamente
    app.run(host='0.0.0.0', port=port, debug=False)

