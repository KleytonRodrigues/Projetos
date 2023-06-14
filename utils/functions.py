from datetime import datetime
from pandas import merge

def get_ultima_rodada(df):
    """
    Obtém a última rodada de cada temporada e adiciona ao dataframe.
    
    Args:
        df (pandas.DataFrame): DataFrame contendo os dados do campeonato.
    
    Returns:
        pandas.DataFrame: DataFrame com a coluna 'ultima_rodada' adicionada.
    """
    df_max_rodada = df[['temporada', 'rodada']].drop_duplicates().groupby('temporada', as_index=False)['rodada'].max()
    df_max_rodada.rename(columns={'rodada': 'ultima_rodada'}, inplace=True)

    return merge(df, df_max_rodada, how='left', on='temporada')

def get_campeao(df):
    """
    Obtém as informações do time campeão de cada temporada e adiciona ao dataframe.
    
    Args:
        df (pandas.DataFrame): DataFrame contendo os dados do campeonato.
    
    Returns:
        pandas.DataFrame: DataFrame com a coluna 'campeao' adicionada.
    """
    df_campeao = df[df['rodada'] == df['ultima_rodada']].query('posicao == 1')[['time', 'temporada', 'posicao']]
    df_campeao.rename(columns={'posicao': 'campeao'}, inplace=True)
    df_campeao = df_campeao[df_campeao['temporada'] != datetime.now().year]

    df_campeao.groupby('time')['campeao'].sum().sort_values(ascending=False)

    df = merge(df, df_campeao, how='left', on=['time', 'temporada'])
    df.fillna({'campeao': 0}, inplace=True)

    return df

def get_rebaixamento(df):
    """
    Obtém as informações dos times rebaixados em cada temporada e adiciona ao dataframe.
    
    Args:
        df (pandas.DataFrame): DataFrame contendo os dados do campeonato.
    
    Returns:
        pandas.DataFrame: DataFrame com as colunas 'z4_1_anos', 'z4_2_anos' e 'z4_3_anos' adicionadas.
    """
    for i in range(1, 4):
        df_z4 = df[df['rodada'] == df['ultima_rodada']].groupby('temporada').tail(4)[['time', 'temporada']]
        df_z4['temporada'] = df_z4['temporada'] + (1 + i)
        df_z4[f'z4_{i}_anos'] = 1
        df = merge(df, df_z4, how='left', on=['time', 'temporada'])
        df.fillna({f'z4_{i}_anos': 0}, inplace=True)
    
    return df

def get_xy(df, values, features, target):
    """
    Separa o DataFrame em matrizes X e y com base nos valores de temporada, recursos (features) e alvo (target) especificados.
    
    Args:
        df (pandas.DataFrame): DataFrame contendo os dados.
        values (list): Lista dos valores de temporada a serem considerados.
        features (list): Lista das colunas de recursos (features) a serem incluídas na matriz X.
        target (str): Nome da coluna alvo (target) a ser incluída na matriz y.
    
    Returns:
        tuple: Tupla contendo as matrizes X e y.
    """
    mask = df['temporada'].isin(values)
    X = df.loc[mask, features].copy()
    y = df.loc[mask, target].copy()
    return X, y
