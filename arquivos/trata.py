import datetime
from datetime import date, timedelta, datetime
import pandas as pd
import os
import os.path
import sys
import json
import time
import threading
import yfinance as yf


class RequiThread(threading.Thread):
    def __init__(self, idt, nome, requi):
        threading.Thread.__init__(self)
        self.idt = idt
        self.nome = nome
        self.requi = requi

    def run(self):
        print('----- Iniciando Thread {} -----' .format(self.nome))
        threads_run(self.idt, self.nome, self.requi)
        print('----- Fim Thread {} -----' .format(self.nome))


def Request(requi):
    print("=====Requisição=====")
    try:
        requi = json.load(requi)
        cont = 0
        for lista in requi['codigos']:
            cont += 1
            thread = RequiThread(cont, str(lista), requi)
            thread.start()

    except Exception as e:
        print(e)


def threads_run(idt, nome, requi):
    try:
        print('----- Iniciando Thread {} -----' .format(nome))
        req_count = 0
        yf_name = yf.Ticker(nome)
        json_req = yf_name.history(period="1d")
        function_reqs(json_req, nome)
        print('===== Fim da Execução =====')

    except Exception as e:
        print(e)


def function_reqs(json_req, nome):
    json_not_edit(json_req, nome)
    tratamento(json_req, nome)


def json_not_edit(json_req, nome):
    try:
        print('===== Conversão Parquet =====')

        caminho_sys = os.path.join("/output", "parquet")

        pasta = os.getcwd() + caminho_sys
        if not os.path.exists(pasta):
            if os.path.isdir(pasta):
                print('Ja existe uma pasta com esse nome!')
            else:
                os.makedirs(pasta, mode=0o777, exist_ok=False)
                print('===== Pasta criada com sucesso! =====')

        try:
            date = datetime.today()
            df = json_req
            df.to_parquet(os.getcwd()+os.path.join("/output", "parquet/") + nome+"-" +
                          date.strftime("%Y%m%d")+'.parquet')

        except Exception as e:
            print("===== Ocorreu um Erro na Conversão do Parquet =====")
            print(e)
            sys.exit()

    except Exception as e:
        print(e)


def tratamento(json_req, nome):
    try:
        print('===== Tratamento =====')
        json_req = dict(json_req)

        data = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        new_df = {
            'Simbolo': nome,
            'Data': data,
            'Abertura': json_req["Open"],
            'Maxima': json_req["High"],
            'Minima': json_req["Low"],
            'Fechamento': json_req["Close"],
            'Volume': json_req["Volume"]
        }

        

        pasta = os.getcwd()+'/output/csv'
        if not os.path.exists(pasta):
            if os.path.isdir(pasta):
                print('Ja existe uma pasta com esse nome!')
            else:
                os.makedirs(pasta, mode=0o777, exist_ok=False)
                print('===== Pasta criada com sucesso! =====')

        try:
            date = datetime.today()
            df = pd.DataFrame.from_dict(new_df)
            df.to_csv(os.getcwd()+'/output/csv/' + nome+"-" +
                      date.strftime("%Y%m%d")+'.csv')

        except Exception as e:
            print("===== Ocorreu um Erro na Conversão do Csv =====")
            print(e)
            sys.exit()

    except Exception as e:
        print('===== Erro Tratamento =====')
        print(e)
