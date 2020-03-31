# Dados da COVID-19 no Rio Grande do Norte

## Formato dos dados

Os dados da COVID-19 disponibilizados neste repositório seguem o padrão adotado pela John Hopkins University, na medida do possível.

## Origem dos dados

Os dados disponíveis nesta página foram extraídos dos boletins epidemiológicos divulgados pela SESAP-RN. A extração manual deve ser creditada a:
- [brasil.io](https://brasil.io): casos confirmados até 24/03/2020
- [vitor-saldanha](https://github.com/vitor-saldanha): casos suspeitos até 24/03/2020
- [leobezerra](https://github.com/leobezerra): latitude e longitude dos municípios e dados posteriores a 24/03/2020

A partir do dia 29/03/2020, a extração passou a ser feita de forma automatizada, utilizando o scraper que deve ser creditado a:
- [gabicavalcante](https://github.com/gabicavalcante): autoria
- [leobezerra](https://github.com/leobezerra): revisão e validação

## Informações sobre o parser

No momento, o parser desenvolvido raspa os casos suspeitos e confirmados apresentados na Tabela 1. Adicionalmente, o parser pode adicionar coordenadas geográficas ao CSV produzido.

- script: `data/rn-parser.py`
- dependências: `data/requirements.py`
- arquivo base de coordenadas: data/coordenadas-rn.csv

## Erratas:

- 31/03/2020
 . Os dados importados não estão sendo contabilizados. Correção em breve.

- 29/03/2020
 . Um caso suspeito no município de Santana do Matos relatado no boletim número 13 (24/03/2020) não havia sido identificado pela coleta manual de dados.
 . Um caso confirmado no município de Monte Alegre e um no município de Passa-e-fica relatados no boletim número 18 (28/03/2020) haviam sido incluídos apenas no arquivo de séries tempoaris.
 . Casos suspeitos do município de São Gonçalo do Amarante relatados no boletim número 16 (27/03/2020) não haviam sido identificados pela coleta manual de dados.
 . Extração do boletim número 17 (28/03/2020) havia sido erroneamente relatada como sendo do boletim número 16 (27/03/2020).
 . Extração do boletim número 15 (25/03/2020) havia sido erroneamente relatado como sendo do dia 26/03/2020.
