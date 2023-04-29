import time
import os
import sys
import pandas as pd
import pymysql
from pathlib import Path
import datetime


class MessageSolver:
    """Deal with modbus message"""
    def __init__(self):
        self.area = ['wuwei', 'lanzhou']

    def write_to_mysql(self, values: list):
        """Write values to mysql table with pymysql library"""
        conn = pymysql.connect(host='124.220.27.50', port=3306, user='daryl', password='ms@imws.C0M',
                               db='cy_soil_hum2023', charset='utf8')
        cur = conn.cursor()

        # check record existence
        try:
            check_sql = """SELECT * FROM cy_soil_hum2023.scholar2023 WHERE area_datetime = %s"""
            res = cur.execute(check_sql, values[1])
            if res != 0:  # records exist
                return None

            sql = f"""INSERT INTO cy_soil_hum2023.scholar2023
            (datetime,area_datetime,area,temp1,rh1,e_const1,temp2,rh2,e_const2,temp3,rh3,e_const3,rain)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            cur.execute(sql, values)  # TODO: uncomment this line
            conn.commit()
        except Exception as e:
            print(values)
            print('Error when writing to sql:', e)
        finally:
            cur.close()
            conn.close()

    def get_hum(self, string: str):
        """Get soil humidity from hex string"""
        try:
            temper = int(string[6:10], 16)/100
            humidity = int(string[10:14])/100
            ele = int(string[14:18], 16)/100
            return [temper, humidity, ele]
        except:
            return [-999, -999, -999]

    def get_rain(self, string: str):
        """Get rainfall from hex string"""
        try:
            rain = int(string[6:10], 16)/10
            return [rain]
        except:
            return [-999]

    def get_log(self, area):
        """Get un uploaded records"""
        logfile = Path(f'./message_log_{area}.csv')
        # Check file modification datetime
        date_modified = logfile.stat().st_mtime
        date_modified = datetime.datetime.fromtimestamp(date_modified)
        time_delta_minutes = (datetime.datetime.now() - date_modified).total_seconds()/60

        # if time_delta_minutes <= 3:
        #     return None

        df = pd.read_csv(logfile)
        df['datetime'] = pd.to_datetime(df['datetime'])

        df['addr'] = df['string'].str.slice(0, 2)
        df = df.loc[df['addr'].isin(['01', '02', '03', '04'])]

        df['total_sec'] = (datetime.datetime.now() - df['datetime']).dt.total_seconds()
        df['dtime'] = df['total_sec'].diff()
        df = df.fillna(0)
        df['dtime'] = df.apply(lambda row: 0 if row['dtime'] > -300 else 1, axis=1)
        df['dtime'] = df['dtime'].cumsum()
        df.drop_duplicates(subset=['addr', 'dtime'], keep='last', inplace=True)
        # df = df.loc[df.minutes <= 60]  # TODO: add this line

        dfg = df.groupby('dtime')
        dfs = [dfg.get_group(label) for label in dfg.groups.keys()]

        return dfs

    def get_record(self, df, area):
        """Get a single record from dataframe"""
        if df is None:
            return None
        area_datetime = None
        record = {}
        datetime_ = df['datetime'].iloc[0]
        for index, row in df.iterrows():
            addr = row['addr']
            if addr == '04':  # rain
                record['04'] = (self.get_rain(row['string']))
                area_datetime = row['area_datetime']
                area = area_datetime.split('_')[1]
            else:
                record[f'{addr}'] = self.get_hum(row['string'])

        for addr in ['01', '02', '03', '04']:
            if addr not in record.keys():

                record[addr] = [-999, -999, -999] if addr != '04' else [-999]

        record_lst = [datetime_, area_datetime, area]
        for k, v in record.items():
            record_lst.extend(v)
        return record_lst

    def write_area(self, area):
        """Write records to sql"""
        dfs = self.get_log(area)
        if dfs is None:  # no new records
            return None
        for df in dfs:
            try:
                if len(df) != 4:
                    continue
                record_lst = self.get_record(df, area)
                self.write_to_mysql(record_lst)
            except Exception as e:
                print('Error when writing area:', e)
            finally:
                time.sleep(0.1)

    def run(self):
        """Run"""
        for area in self.area:
            self.write_area(area)


def main():
    ms = MessageSolver()
    ms.run()
    exit()
    while True:
        try:
            ms = MessageSolver()
            ms.run()
        except Exception as e:
            print('Error occured when writing message:', e)
        time.sleep(60*4)


if __name__ == '__main__':
    main()
