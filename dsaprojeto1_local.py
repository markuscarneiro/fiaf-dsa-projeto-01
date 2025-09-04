# Projeto 1 - Pipeline Financeiro - Automatizando Coleta, Limpeza e Armazenamento de Dados de Mercado em Tempo Real

# Imports
import pandas as pd
import yfinance as yf
import sqlite3
from datetime import datetime
import logging

# Configuração de Log
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("dados_local/dsapipeline.log"),
        logging.StreamHandler()
    ]
)

# Lista de ações
ACOES_PARA_ACOMPANHAR = [
    'PETR4.SA', 'VALE3.SA', 'ITUB4.SA', 'BBDC4.SA', 
    'ABEV3.SA', 'MGLU3.SA', 'WEGE3.SA', 'VLTA.CN'
]

# Nome do banco de dados e da tabela
NOME_BANCO_DADOS = 'dados_local/dsa_dados_mercado.db'
NOME_TABELA = 'dados_acoes_diario'

# Função para criar (primeira execução) ou conectar no banco de dados
def dsa_cria_ou_conecta_banco():

    try:

        # Conecta ao banco de dados usando o nome definido em NOME_BANCO_DADOS
        conn = sqlite3.connect(NOME_BANCO_DADOS)
        
        # Cria um cursor para executar comandos SQL
        cursor = conn.cursor()
        
        # Executa a criação da tabela, caso ela ainda não exista
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {NOME_TABELA} (
            ticker TEXT NOT NULL,                 -- Código da ação
            data_pregao DATE NOT NULL,            -- Data do pregão
            abertura REAL,                        -- Valor de abertura
            alta REAL,                            -- Valor mais alto do dia
            baixa REAL,                           -- Valor mais baixo do dia
            fechamento REAL,                      -- Valor de fechamento
            volume INTEGER,                       -- Volume negociado
            datetime_coleta TEXT,                 -- Data e hora da coleta dos dados
            PRIMARY KEY (ticker, data_pregao)     -- Chave primária composta
        );
        """)
        
        # Salva (confirma) as alterações no banco de dados
        conn.commit()
        
        # Registra em log que o banco e a tabela foram criados ou verificados
        logging.info(f"Banco de dados '{NOME_BANCO_DADOS}' e tabela '{NOME_TABELA}' verificados/criados com sucesso.")
        
        # Retorna o objeto de conexão para uso posterior
        return conn
    
    except sqlite3.Error as e:

        # Em caso de erro, registra o erro no log
        logging.error(f"Erro ao criar/conectar ao banco de dados: {e}")
        
        # Retorna None para indicar falha na conexão
        return None

# Define a função para extrair os dados mais recentes de um determinado ticker
def dsa_extrai_dados_acao(ticker):

    try:

        # Registra no log o início da extração de dados para o ticker fornecido
        logging.info(f"Extraindo dados para o ticker: {ticker}...")
        
        # Cria um objeto Ticker usando o módulo yfinance
        acao = yf.Ticker(ticker)
        
        # Obtém o histórico de preços da ação para o período de 5 dias
        dados = acao.history(period='5d')
        
        # Verifica se os dados estão vazios (sem resultados)
        if dados.empty:

            # Registra um aviso no log caso nenhum dado tenha sido retornado
            logging.warning(f"Não foram encontrados dados para o ticker {ticker}.")
            return None
        
        # Retorna os dados extraídos
        return dados

    except Exception as e:

        # Registra no log qualquer exceção que tenha ocorrido durante a extração
        logging.error(f"Falha ao extrair dados para {ticker}: {e}")
        
        # Retorna None em caso de erro
        return None

# Define a função para limpar e transformar os dados brutos extraídos para o formato desejado
def dsa_limpa_e_transforma_dados(df_bruto, ticker):

    # Verifica se o DataFrame de entrada é None ou está vazio
    if df_bruto is None or df_bruto.empty:
        return None
    
    # Registra no log o início da transformação dos dados para o ticker informado
    logging.info(f"Transformando dados para o ticker: {ticker}...")
    
    # Reseta o índice do DataFrame para transformar a coluna 'Date' em coluna normal
    df_transformado = df_bruto.reset_index()
    
    # Adiciona a coluna 'ticker' com o valor correspondente
    df_transformado['ticker'] = ticker
    
    # Adiciona a coluna com o timestamp da coleta dos dados
    df_transformado['datetime_coleta'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Renomeia as colunas para nomes padronizados em português
    df_transformado.rename(columns={
        'Date': 'data_pregao',
        'Open': 'abertura',
        'High': 'alta',
        'Low': 'baixa',
        'Close': 'fechamento',
        'Volume': 'volume'
    }, inplace=True)
    
    # Define a lista de colunas que devem estar no DataFrame final
    colunas_desejadas = ['ticker', 'data_pregao', 'abertura', 'alta', 'baixa', 'fechamento', 'volume', 'datetime_coleta']
    
    # Cria uma cópia do DataFrame contendo apenas as colunas desejadas e que existem no DataFrame transformado
    df_final = df_transformado[[col for col in colunas_desejadas if col in df_transformado.columns]].copy()
    
    # Converte a coluna 'data_pregao' para o tipo date (sem hora), garantindo consistência no formato
    df_final['data_pregao'] = pd.to_datetime(df_final['data_pregao']).dt.date
    
    # Retorna o DataFrame final transformado
    return df_final

# Define a função responsável por carregar os dados transformados no banco de dados SQLite
def dsa_carrega_dados(df, conn):

    # Verifica se o DataFrame está vazio ou é None
    if df is None or df.empty:

        # Registra um aviso no log e encerra a função se não houver dados a carregar
        logging.warning("DataFrame vazio, nada para carregar no banco de dados.")
        return

    try:

        # Cria um cursor para executar comandos SQL no banco de dados
        cursor = conn.cursor()
        
        # Itera sobre as linhas do DataFrame, uma por uma
        for _, row in df.iterrows():

            # Define a query SQL para inserir ou atualizar os dados na tabela
            query = f"""
            INSERT INTO {NOME_TABELA} (ticker, data_pregao, abertura, alta, baixa, fechamento, volume, datetime_coleta)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ticker, data_pregao) DO UPDATE SET
                abertura=excluded.abertura,
                alta=excluded.alta,
                baixa=excluded.baixa,
                fechamento=excluded.fechamento,
                volume=excluded.volume,
                datetime_coleta=excluded.datetime_coleta;
            """

            # Extrai os valores da linha atual como uma tupla
            valores = tuple(row)
            
            # Executa a query com os valores fornecidos
            cursor.execute(query, valores)
        
        # Confirma (salva) todas as operações realizadas no banco de dados
        conn.commit()
        
        # Obtém o ticker da primeira linha do DataFrame para registro no log
        ticker_log = df['ticker'].iloc[0]
        
        # Registra no log que os dados foram carregados/atualizados com sucesso
        logging.info(f"Dados para o ticker '{ticker_log}' carregados/atualizados com sucesso.")
    
    except sqlite3.Error as e:

        # Registra o erro ocorrido no log
        logging.error(f"Erro ao carregar dados no banco de dados: {e}")
        
        # Desfaz qualquer alteração feita na transação atual em caso de erro
        conn.rollback()

# Define a função principal que orquestra toda a execução do pipeline financeiro
def main():

    # Registra no log o início da execução do pipeline
    logging.info("--- INICIANDO A EXECUÇÃO DO PIPELINE FINANCEIRO ---")
    
    # Cria ou conecta ao banco de dados
    conn = dsa_cria_ou_conecta_banco()
    
    # Se não for possível conectar, registra erro crítico e encerra a execução
    if conn is None:
        logging.critical("Não foi possível estabelecer conexão com o banco de dados. Encerrando o pipeline.")
        return
    
    # Itera sobre a lista de ações a serem monitoradas
    for ticker in ACOES_PARA_ACOMPANHAR:

        # Extrai os dados brutos do ticker atual
        dados_brutos = dsa_extrai_dados_acao(ticker)
        
        # Aplica limpeza e transformação aos dados extraídos
        dados_limpos = dsa_limpa_e_transforma_dados(dados_brutos, ticker)
        
        # Se os dados estiverem prontos para carga, insere no banco
        if dados_limpos is not None:
            dsa_carrega_dados(dados_limpos, conn)
        else:
            # Caso não haja dados, registra aviso no log
            logging.warning(f"Processamento pulado para o ticker {ticker} devido à ausência de dados.")
    
    # Fecha a conexão com o banco de dados ao final do pipeline
    conn.close()
    
    # Registra no log que a conexão foi encerrada
    logging.info(f"Conexão com o banco de dados '{NOME_BANCO_DADOS}' fechada.")
    
    # Registra no log o fim da execução do pipeline
    logging.info("--- PIPELINE EXECUTADO COM SUCESSO ---")

# Bloco principal de execução do script
if __name__ == '__main__':
    main()

