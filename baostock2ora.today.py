import baostock as bs
import pandas as pd
import os
# import cx_Oracle
import time
from datetime import datetime
from sqlalchemy import create_engine, types

# http://baostock.com/baostock/index.php/A%E8%82%A1K%E7%BA%BF%E6%95%B0%E6%8D%AE

#os.environ['TNS_ADMIN'] = 'd:/oracle/product/11.2.0/client/network/admin/'

os.environ['TNS_ADMIN'] = '/home/opc/Wallet_atp/'
#engine = create_engine('oracle://mytomcatapp1:AAbb##444AAbb##444@atp_high',max_identifier_length=128)
engine = create_engine('oracle://mytomcatapp1:TGByhn#258@atp_high',max_identifier_length=30)
print(engine)

#engine = create_engine(
#'oracle+cx_oracle://scott:tiger@RACDB12PDB1', max_identifier_length=30)

# con = cx_Oracle.connect(p_username, p_password, p_service)

'''
p_username = 'admin'
p_password = 'AAbb##444AAbb##444'
p_service = 'atp_high'

con = cx_Oracle.connect(p_username, p_password, p_service)
'''


class Downloader(object):
    def __init__(self,
                 date_start='1990-01-01',
                 date_end='2020-03-23'):
        self._bs = bs
        bs.login()
        self.date_start = date_start
        # self.date_end = datetime.datetime.now().strftime("%Y-%m-%d")
        self.date_end = date_end
        self.fields = "date,code,open,high,low,close,volume,amount," \
                      "adjustflag,turn,tradestatus,pctChg,peTTM," \
                      "pbMRQ,psTTM,pcfNcfTTM,isST"

    def exit(self):
        bs.logout()

    def get_codes_by_date(self, date):
        print(date)
        stock_rs = bs.query_all_stock(date)
        stock_df = stock_rs.get_data()
        print(stock_df)
        with engine.connect() as connection:
            connection.execute("delete from dm_baostock")

        stock_df2 = stock_df.copy(deep=True)

        stock_df.columns = ['CODE', 'TRADESTATUS', 'CODE_NAME']
        stock_df['CODE'] = stock_df['CODE'].apply(
           lambda x: str(x[0:2]).upper()+x[3:9])
        stock_df.to_sql('dm_baostock', engine, index=False, if_exists='append', dtype={
                           'CODE': types.VARCHAR(length=8),
                           'TRADESTATUS': types.INTEGER()})
        return stock_df2

    def run(self):
        stock_df = self.get_codes_by_date(self.date_end)
        df = pd.read_sql("select code,code_name from dm_baostock\
             where code not in (\
             select code from realtimestockprice where substr(name,1,2)='XD')" , con=engine)
        #for index, row in stock_df.iterrows():
        for index, row in df.iterrows():
            print(f'processing {row["code"]} {row["code_name"]}')
            start_time=time.time()
            #code = "sh.600037"
            #code = "sh.600081"
            code8 = row["code"]
            code = code8[0:2]+'.'+code8[2:8]
            print(code)
            df_code = bs.query_history_k_data_plus(code, self.fields,
            # adjustflag：复权类型，默认不复权：3；1：后复权；2：前复权。
            #df_code = bs.query_history_k_data_plus(row["code"], self.fields,
            #rs = bs.query_history_k_data_plus(code, self.fields,
                                                   start_date=self.date_start,
                                                   end_date=self.date_end,
                                                   frequency="d", adjustflag="3").get_data()
                                                   #frequency="d", adjustflag="2").get_data()

            #print('query_history_k_data_plus respond error_code:'+rs.error_code)
            #print('query_history_k_data_plus respond error_msg :'+rs.error_msg)
            #df_code = rs.get_data()

            # code_name = row["code_name"].replace('*', '')
            #print(code)
            #code = code.replace('.', '')
            df_code.columns = ['RQ', 'CODE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'AMOUNT',
                               'ADJUSTFLAG', 'TURN', 'TRADESTATUS', 'PCTCHG', 'PETTM', 'PBMRQ',
                               'PSTTM', 'PCFNCFTTM', 'ISST']
            #print(df_code.columns)
            df_code['RQ'] = pd.to_datetime(
                df_code['RQ'], format='%Y-%m-%d')
            df_code['CODE'] = code8
            # df_code['CODE'].apply(
            #    lambda x: str(x[0:2]).upper()+x[3:9])
            df_code['VOLUME'].replace('', '0', inplace=True)
            df_code['AMOUNT'].replace('', '0', inplace=True)
            df_code['TURN'].replace('', '0', inplace=True)
            df_code['PCTCHG'].replace('', '0', inplace=True)
            df_code['PETTM'].replace('', '0', inplace=True)
            df_code['PBMRQ'].replace('', '0', inplace=True)
            df_code['PSTTM'].replace('', '0', inplace=True)
            df_code['PCFNCFTTM'].replace('', '0', inplace=True)
            df_code['ISST'].replace('', '0', inplace=True)
            convert_dict = {'CODE': str,
                            'OPEN': float,
                            'HIGH': float,
                            'LOW': float,
                            'CLOSE': float,
                            'VOLUME': int,
                            'AMOUNT': float,
                            'ADJUSTFLAG': int,
                            'TURN': float, 'TRADESTATUS': int, 'PCTCHG': float, 'PETTM': float, 'PBMRQ': float,
                            'PSTTM': float, 'PCFNCFTTM': float, 'ISST': int
                            }
            print(df_code.head())
            df_code = df_code.astype(convert_dict)
            #print(df_code.head())
            #print(df_code.dtypes)
            print(df_code.tail())
            df_code.to_sql('hq_baostock', engine, index=False, if_exists='append', dtype={
                           'CODE': types.VARCHAR(length=8),
                           'ISST': types.INTEGER()})
            end_time=time.time()
            print('elapsed '+str(end_time-start_time))
            #break
            '''
            , dtype={
                'DATE': types.DateTime(),
                'CODE': types.VARCHAR(length=9),
                'OPEN': types.Float(precision=4, asdecimal=True),
                'HIGH': types.Float(precision=4, asdecimal=True),
                'LOW': types.Float(precision=4, asdecimal=True),
                'CLOSE': types.Float(precision=4, asdecimal=True),
                'VOLUME': types.INTEGER(),
                'AMOUNT': types.Float(precision=4, asdecimal=True),
                'ADJUSTFLAG': types.INTEGER(),
                'TURN': types.Float(precision=6, asdecimal=True),
                'TRADESTATUS': types.INTEGER(),
                'PCTCHG': types.Float(precision=6, asdecimal=True),
                'PETTM': types.Float(precision=6, asdecimal=True),
                'PBMRQ': types.Float(precision=6, asdecimal=True),
                'PSTTM': types.Float(precision=6, asdecimal=True),
                'PCFNCFTTM': types.Float(precision=6, asdecimal=True),
                'ISST': types.INTEGER()})
            '''
            #break
#            df_code.to_csv(
#                f'{self.output_dir}/{row["code"]}.{code_name}.csv', index=False)
        self.exit()


if __name__ == '__main__':
    # 获取全部股票的日K线数据
    now=datetime.now()
    t = now.strftime("%Y-%m-%d")
    #downloader = Downloader(date_start='2020-01-04', date_end='2020-06-05')
    downloader = Downloader(date_start=t, date_end=t)
    downloader.run()
