import requests
from datetime import datetime
from os.path import join
from pandas import read_html, read_csv, to_numeric, concat
from glob import glob

class WorldFootball(object):

    def __init__(self, base_path) -> None:
        self.URL = 'https://www.worldfootball.net'
        self.header={'User-Agent': 'Mozilla/5.0'}
        self.base_path = base_path
        self.datasets_raw = []
        self.datasets_processed = []
        self.dataset_curated = None
        self.ultima_temporada = 1
        self.temporada = datetime.now().year
    
    def run(self, ultima_temporada) -> None:
        print('Rodando ETL')
        self.extract(ultima_temporada=ultima_temporada)
        self.transform(input = 'env')
        self.load(input = 'env')

    def extract(self, ultima_temporada:int = 1, save=True) -> None:
        print('Extracting...', end = ' ')
        self.ultima_temporada=ultima_temporada
        for temporada in range(self.temporada, self.temporada-self.ultima_temporada, -1):

            for rodada in range(1, 39):
                # get raw data
                url = f'{self.URL}/schedule/bra-serie-a-{temporada}-spieltag/{rodada}/'
                r = requests.get(url, headers=self.header)

                dfs = read_html(r.text,header=0)
                file_save = f"df_{temporada}_{rodada}.csv"
                if len(dfs[3]) == 0: # se temporada regular, para o loop na rodada atual
                    break

                # Adicionando variavel para distincao
                df_rw = dfs[3].copy()
                df_rw['temporada'] = temporada

                # Save raw data
                self.datasets_raw.append(df_rw)
                if save:
                    df_rw.to_csv(join(self.base_path, 'data', 'raw', file_save), index = False)
        print('Done!')

    def transform(self, input:str = 'csv') -> None:
        print('Transforming...', end = ' ')
        if input == 'csv' or len(self.datasets_raw) == 0:
            datasets = []
            for csv in glob(join(self.base_path, 'data', 'raw', '*.csv')):
                datasets.append(read_csv(csv))
        else:
            datasets = self.datasets_raw

        for df in datasets:
            df = df.copy()
            df.drop(['Team'], inplace=True, axis=1) # null values, escuto
            df.rename(index=str, columns={'#': 'posicao', 'Team.1': 'time', 'M.': 'rodada', 'W': 'vitoria',
                                            'D': 'empate', 'L': 'derrota', 'goals': 'gols',
                                            'Dif.': 'diferenca_gols', 'Pt.': 'pontos'}, inplace=True)

            df[['gols', 'gols_sofridos']] = df['gols'].str.split(':', n = 1, expand = True)

            df = df.apply(to_numeric, errors='ignore')
            df['posicao'] =  to_numeric(df.index) + 1

            df = df.reindex(columns=[
                'posicao',
                'time',
                'rodada',
                'vitoria',
                'empate',
                'derrota',
                'gols',
                'gols_sofridos',
                'diferenca_gols',
                'pontos',
                'temporada'])
            self.datasets_processed.append(df)
            file_name = f"df_processed_{df['temporada'].iloc[0]}_{df['rodada'].iloc[0]}.csv"
            df.to_csv(join(self.base_path, 'data', 'processed', file_name), index = False)
        print('Done!')

    def load(self, input:str = 'csv') -> None:
        print('loading...', end = ' ')
        if input == 'csv' or len(self.datasets_processed) == 0:
            datasets = []
            for csv in glob(join(self.base_path, 'data', 'processed', '*.csv')):
                datasets.append(read_csv(csv))
        else:
            datasets = self.datasets_processed
        
        self.dataset_curated = concat(datasets)
        file_name = f'df_curated.csv'
        self.dataset_curated.to_csv(join(self.base_path, 'data', 'curated', file_name), index = False)
        print('Done!')
