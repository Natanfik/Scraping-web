import pandas as pd
import requests
from bs4 import BeautifulSoup
from dash import Dash, dash_table, html, dcc
import plotly.express as px
import sqlite3
import datetime
from sqlalchemy import create_engine


def coletar_dados_clima_e_movimentacao(url, porto_nome):
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Erro ao acessar {url}: {response.status_code}")
        return pd.DataFrame()

    soup = BeautifulSoup(response.content, 'html.parser')
    dados = []
    
    # Localiza a tabela no HTML
    tabela = soup.find('table', class_="table table-sm table-borderless mb-2 w-100")
    if tabela:
        # Itera pelas linhas da tabela
        for linha in tabela.find_all('tr'):
            colunas = linha.find_all('td')
            if colunas and len(colunas) > 1:  # Garante que há pelo menos 2 colunas
                try:
                    # Acessa o texto das colunas diretamente
                    tempo = colunas[0].text.strip()  # Exemplo: primeira coluna
                    temperatura = colunas[1].text.strip()  # Exemplo: segunda coluna

                    # Adiciona os dados ao array
                    dados.append({
                        "Tempo": tempo,
                        "Temperatura": temperatura,
                        "Porto": porto_nome,
                    })
                except (ValueError, IndexError) as e:
                    print(f"Erro ao processar linha: {e}")
    else:
        print("Tabela não encontrada na página.")

    # Retorna os dados como um DataFrame do pandas
    return pd.DataFrame(dados)

# Exemplo de uso
url = "https://www.myshiptracking.com/ports/port-of-santos-in-br-brazil-id-369"
porto_nome = "Porto Exemplo"
dados_clima = coletar_dados_clima_e_movimentacao(url, porto_nome)
print(dados_clima)


def gerar_url_dia_atual(porto_id):
    data_atual = datetime.datetime.now()
    inicio_dia = int(datetime.datetime(data_atual.year, data_atual.month, data_atual.day, 0, 0).timestamp())
    fim_dia = int(datetime.datetime(data_atual.year, data_atual.month, data_atual.day, 23, 59).timestamp())
    url = f"https://www.myshiptracking.com/ports-arrivals-departures/?mmsi=&pid={porto_id}&type=0&time={inicio_dia}_{fim_dia}&pp=20"
    return url

# Função para coletar dados do site de tracking
def coletar_dados_porto(url, porto_nome):
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Erro ao acessar {url}: {response.status_code}")
        return pd.DataFrame()

    soup = BeautifulSoup(response.content, 'html.parser')
    dados_porto = []

    tabela = soup.find('table')
    if tabela:
        for linha in tabela.find_all('tr'):
            colunas = linha.find_all('td')
            if colunas:
                try:
                    tempo = colunas[2].text.strip()
                    evento = colunas[1].text.strip()
                    porta = porto_nome
                
                    navio = colunas[4].text.strip()

                    dados_porto.append({
                        "Tempo": tempo,
                        "Sentido": evento,
                        "Porto": porta,
                        "Navio": navio,
                        
                    })
                except (ValueError, IndexError) as e:
                    print(f"Erro ao processar linha: {e}")
    else:
        print("Tabela não encontrada na página.")

    return pd.DataFrame(dados_porto)

def coletar_dados_extras(url_dwt, porto_nome):
    response = requests.get(url_dwt)

    if response.status_code != 200:
        print(f"Erro ao acessar {url_dwt}: {response.status_code}")
        return pd.DataFrame()

    soup = BeautifulSoup(response.content, 'html.parser')
    dados_porto = []
  
    tabela = soup.find('table')
    if tabela:
        for linha in tabela.find_all('tr'):
            colunas = linha.find_all('td')
            if colunas:
                try:
                    dwt = colunas[2].text.strip() if len(colunas) > 2 else 'Não informado'
                    grt = colunas[3].text.strip() if len(colunas) > 2 else 'Não informado'
                    chegado = colunas[1].text.strip() if len(colunas) > 2 else 'Não informado'
                    tamanho = colunas[6].text.strip() if len(colunas) > 6 else 'Não informado'
                    porta = porto_nome
                    navio = colunas[0].text.strip() if len(colunas) > 0 else 'Não informado'

                    dados_porto.append({
                        "DWT": dwt,
                        "Tamanho": tamanho,
                        "GRT": grt,
                        "Chegado": chegado,
                        "Porto": porta,
                        "Navio": navio,
                        
                    })
                except (ValueError, IndexError) as e:
                    print(f"Erro ao processar linha: {e}")
    else:
        print("Tabela não encontrada na página.")

    return pd.DataFrame(dados_porto)

def gerar_url_extras():
    return "https://www.myshiptracking.com/inport?pid=6703"


# URLs dinâmicas para os portos de Paranaguá e Santos
url_paranagua = gerar_url_dia_atual(6703)
url_santos = gerar_url_dia_atual(369)

# URLs dinâmicas para os portos de Paranaguá e Santos
url_extra_santos = gerar_url_extras()
url_extra_paranagua = gerar_url_extras()

# Coleta de dados dos portos
dados_extras_paranagua = coletar_dados_extras(url_extra_santos, "Paranaguá")
dados_extras_santos = coletar_dados_extras(url_extra_santos, "santos")

# Concatena dados dos portos
dados_extras = pd.concat([dados_extras_paranagua,dados_extras_santos ], ignore_index=True)

# Coleta de dados dos portos
dados_paranagua = coletar_dados_porto(url_paranagua, "Paranaguá")
dados_santos = coletar_dados_porto(url_santos, "Santos")



# Concatena dados dos portos
dados = pd.concat([dados_paranagua, dados_santos], ignore_index=True)
dados['Sentido'] = dados['Sentido'].replace({'Departure': 'partida', 'Arrival': 'chegada'})
dados['Tempo'] = pd.to_datetime(dados['Tempo'], errors='coerce')
dados['Hora'] = dados['Tempo'].dt.hour

# Agrupando dados
movimentacao_hora = dados.groupby(['Hora', 'Sentido']).size().reset_index(name='Movimentacao')
comparacao_porto = dados.groupby('Porto').size().reset_index(name='Contagem')


# gráficos
fig1 = px.line(movimentacao_hora, x='Hora', y='Movimentacao', color='Sentido', title="Movimentação por Hora e Sentido")
fig2 = px.pie(comparacao_porto, names='Porto', values='Contagem', title="Distribuição por Porto")

movimentacao_chegada = dados[dados['Sentido'] == 'chegada'].groupby('Navio').size().reset_index(name='Movimentacao').sort_values(by='Movimentacao', ascending=False)
movimentacao_partida = dados[dados['Sentido'] == 'partida'].groupby('Navio').size().reset_index(name='Movimentacao').sort_values(by='Movimentacao', ascending=False)

fig_chegada = px.bar(movimentacao_chegada, x='Movimentacao', y='Navio', orientation='h', title="Movimentação de Chegadas por Navio")
fig_partida = px.bar(movimentacao_partida, x='Movimentacao', y='Navio', orientation='h', title="Movimentação de Partidas por Navio")


# Conexão com o banco de dados SQLite (ou crie o banco, se não existir)
conn = sqlite3.connect("portos.db")

# Criar tabelas (executado apenas uma vez para criar a estrutura do banco)
def criar_tabelas():
    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS movimentacoes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tempo TEXT,
                sentido TEXT,
                porto TEXT,
                navio TEXT,
                hora INTEGER,
                data_coleta DATE
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS volume (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dwt TEXT,
                tamanho TEXT,
                grt TEXT,
                chegado TEXT,
                porto TEXT,
                navio TEXT,
                data_coleta DATE
            );
        """)

# Chamada para criar as tabelas
criar_tabelas()

# Salvar dados de movimentação no banco
def salvar_dados_movimentacao(dados):
    try:
        if not dados.empty:
            dados['data_coleta'] = datetime.date.today()
            dados.to_sql('movimentacoes', conn, if_exists='append', index=False)
            print("Dados de movimentacao salvos com sucesso!")
        else:
            print("Nenhum dado de movimentacao para salvar.")
    except Exception as e:
        print(f"Erro ao salvar dados de movimentacao: {e}")

# Salvar dados de volume no banco
def salvar_dados_volume(dados_extras):
    try:
        if not dados_extras.empty:
            dados_extras['data_coleta'] = datetime.date.today()
            dados_extras.to_sql('volume', conn, if_exists='append', index=False)
            print("Dados de volume salvos com sucesso!")
        else:
            print("Nenhum dado de volume para salvar.")
    except Exception as e:
        print(f"Erro ao salvar dados de volume: {e}")

# Salvar os dados no banco de dados
salvar_dados_movimentacao(dados)
salvar_dados_volume(dados_extras)

# Confirmar as alterações no banco de dados
conn.commit()

cursor = conn.cursor()
# Consultar os dados reais para substituição

# Confirmar as alterações no banco de dados

# Consultar os dados reais para substituição
movimentacao_df = pd.read_sql_query("SELECT * FROM movimentacoes", conn)
volume_df = pd.read_sql_query("SELECT * FROM volume", conn)

# Exibir os dados para verificar
print("Dados de Movimentação:")
print(movimentacao_df)
print("\nDados de Volume:")
print(volume_df)

# Fechar a conexão
conn.close()

# Configura o Dash
app = Dash(__name__)

app.layout = html.Div(style={
    'padding': '20px',
    'backgroundColor': '#f4f4f4'
}, children=[
    html.H1("Veeries - Dashboard", style={'textAlign': 'center', 'color': '#120a8f'}),
    
        # Divisão principal com duas colunas
        # Tabela
        html.Div(dash_table.DataTable(
            data=dados_clima.to_dict("records"),
            columns=[{"name": i, "id": i} for i in dados_clima.columns],
            page_size=1,
            style_table={'overflowX': 'auto', 'border': 'thin lightgrey solid', 'borderRadius': '8px'},
            style_cell={'textAlign': 'center', 'padding': '5px', 'border': '1px solid #ddd', 'fontSize': '12px'},
            style_header={'backgroundColor': '#120a8f', 'color': 'white', 'fontWeight': 'bold', 'fontSize': '14px'},
            style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': '#f9f9f9'}]
        ), style={'borderRadius': '8px', 'boxShadow': '0px 4px 10px rgba(0, 0, 0, 0.1)', 'marginTop': '20px', 'width': '465px'}),
        html.Div(dash_table.DataTable(
            data=dados.to_dict("records"),
            columns=[{"name": i, "id": i} for i in dados.columns],
            page_size=10,
            style_table={'overflowX': 'auto', 'border': 'thin lightgrey solid', 'borderRadius': '8px'},
            style_cell={'textAlign': 'center', 'padding': '5px', 'border': '1px solid #ddd', 'fontSize': '12px'},
            style_header={'backgroundColor': '#120a8f', 'color': 'white', 'fontWeight': 'bold', 'fontSize': '14px'},
            style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': '#f9f9f9'}]
        ), style={'borderRadius': '8px', 'boxShadow': '0px 4px 10px rgba(0, 0, 0, 0.1)', 'marginTop': '20px', 'width': 'auto'}),

    # Gráficos fig1 e fig2 abaixo da tabela
    html.Div(style={'display': 'grid', 'gridTemplateColumns': '1fr 1fr', 'gap': '20px'}, children=[
        html.Div(dcc.Graph(figure=fig1, config={'displayModeBar': False}), style={
            'border': '1px solid #ddd',
            'borderRadius': '8px',
            'boxShadow': '0px 4px 10px rgba(0, 0, 0, 0.1)',
            'backgroundColor': 'white',
            'overflow': 'hidden',
            'marginTop': '20px',
        }),
        html.Div(dcc.Graph(figure=fig2, config={'displayModeBar': False}), style={
            'border': '1px solid #ddd',
            'borderRadius': '8px',
            'boxShadow': '0px 4px 10px rgba(0, 0, 0, 0.1)',
            'backgroundColor': 'white',
            'overflow': 'hidden',
            'marginTop': '20px',
        })
    ]),
    # Divisão principal com duas colunas
    html.Div(style={'display': 'grid', 'gridTemplateColumns': '1fr 1fr', 'gap': '20px'}, children=[
        # Tabela
        html.Div(dash_table.DataTable(
            data=dados_extras.to_dict("records"),
            columns=[{"name": i, "id": i} for i in dados_extras.columns],
            page_size=10,
            style_table={'overflowX': 'auto', 'border': 'thin lightgrey solid', 'borderRadius': '8px'},
            style_cell={'textAlign': 'center', 'padding': '5px', 'border': '1px solid #ddd', 'fontSize': '12px'},
            style_header={'backgroundColor': '#120a8f', 'color': 'white', 'fontWeight': 'bold', 'fontSize': '14px'},
            style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': '#f9f9f9'}]
        ), style={'borderRadius': '8px', 'boxShadow': '0px 4px 10px rgba(0, 0, 0, 0.1)', 'marginTop': '20px', 'width': '465px'}),

        # Divisão para os gráficos de colunas horizontais
        html.Div(style={'display': 'grid', 'gridTemplateColumns': '1fr 1fr', 'gap': '20px','width': '490px','overflowX': 'auto','widthMax': '470px'}, children=[
            html.Div(dcc.Graph(figure=fig_chegada, config={'displayModeBar': False}), style={
                'border': '1px solid #ddd',
                'borderRadius': '8px',
                'boxShadow': '0px 4px 10px rgba(0, 0, 0, 0.1)',
                'backgroundColor': 'white',
                'overflowX': 'auto',
                'marginTop': '20px',
                'width': 'calc(100% + 10px)' 
            }),
            html.Div(style={
                'border': '1px solid #ddd',
                'borderRadius': '8px',
                'boxShadow': '0px 4px 10px rgba(0, 0, 0, 0.1)',
                'backgroundColor': 'white',
                'width': '220px',
                'overflowX': 'auto',
                'marginTop': '20px'
            }, children=[
                dcc.Graph(figure=fig_partida, config={'displayModeBar': False})
            ]),
        ])
    ]),
])

if __name__ == "__main__":
    app.run_server(debug=True)
