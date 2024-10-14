import tushare as ts
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import time

# 兄弟模块的导入
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from common.get_config import Config


class GetData:
    def __init__(self, tushare_token, database='stock'):
        # 设置tushare配置
        self.tushare_token = tushare_token
        self.pro = ts.pro_api(tushare_token)
        # 设置mysql配置
        self.config = Config()
        self.mysql_config = {'mysql_host': self.config['mysql_host'], 'mysql_port': self.config['mysql_port'],
                             'mysql_username': self.config['mysql_username'], 'mysql_password': self.config['mysql_password']}
        db = pymysql.connect(host=self.mysql_config['mysql_host'], user=self.mysql_config['mysql_username'],
                                password=self.mysql_config['mysql_password'], database=database)
        self.cursor = db.cursor()
        self.engine = create_engine(f'mysql+pymysql://{self.mysql_config["mysql_username"]}:{self.mysql_config["mysql_password"]}@{self.mysql_config["mysql_host"]}:{self.mysql_config["mysql_port"]}/{database}')
        # 获取交易日历
        self.trade_calendar = self.get_trade_calendars('20000101', '20241013')
        self.trade_calendar.set_index('cal_date', inplace=True)

    def get_stock_data_by_tushare(self, ts_codes, start_date, end_date, **kwargs):
        # '**kwargs: 用于接收tushare pro获取数据时的可变参数'
        # 以tscode为表名，寻找是否有表名为tscode的表，如果没有则创建表并且获取数据，如果有则下一步
        temp_df = None
        if ',' in ts_codes:
            ts_codes = ts_codes.split(',')
        else:
            ts_codes = [ts_codes]
        for ts_code in ts_codes:
            alias_table = ts_code.replace('.', '_').lower() # 没有小写跟.-->_的处理是为了防止查询出错
            sql = "SHOW TABLES LIKE %s"
            self.cursor.execute(sql, (alias_table,))
            result = self.cursor.fetchall()
            if not result:
                # 如果没有表名为ts_code的表，则创建表
                df = self._get_stock_data_by_tushare(ts_code, start_date, end_date, **kwargs)
                # 把日期从小到大排序
                df = df.sort_values(by='trade_date')
                df.to_sql(alias_table, con=self.engine, if_exists='append', index=False)
            else:
                # 寻找数据库中交易时间的最大值和最小值
                sql = 'SELECT MIN(trade_date), MAX(trade_date) FROM %s' % alias_table
                self.cursor.execute(sql)
                result = self.cursor.fetchall()
                min_date, max_date = result[0]
                # 如果数据库中的数据没有覆盖到start_date和end_date，则获取数据并且插入
                if start_date < min_date and self.check_is_trade_day(start_date, min_date):
                    df = self._get_stock_data_by_tushare(ts_code, start_date, min_date, **kwargs)
                    df.to_sql(alias_table, con=self.engine, if_exists='append', index=False)
                if end_date > max_date and self.check_is_trade_day(self.get_nextday(max_date), self.get_nextday(max_date)):
                    df = self._get_stock_data_by_tushare(ts_code, max_date, end_date, **kwargs)
                    df.to_sql(alias_table, con=self.engine, if_exists='append', index=False)
                # 对数据库中的数据重新排序
                df = pd.read_sql("SELECT * FROM %s" % alias_table, con=self.engine)
                df = df.sort_values(by='trade_date')
                self.cursor.execute('drop table %s' % alias_table)
                df.to_sql(alias_table, con=self.engine, if_exists='append', index=False)
            # 获取数据返回
            sql = 'SELECT * FROM %s WHERE trade_date >= %s AND trade_date <= %s'
            df = pd.read_sql(sql % (alias_table, start_date, end_date), con=self.engine)
            temp_df = df if temp_df is None else pd.concat([temp_df, df])
        return temp_df

    def _get_stock_data_by_tushare(self, ts_code, start_date, end_date, **kwargs):
        # '**kwargs: 用于接收tushare pro获取数据时的可变参数'
        print('正在从tushare获取数据-->(ts_code: %s, start_date: %s, end_date: %s)' % (ts_code, start_date, end_date))
        df = self.pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date, **kwargs)
        time.sleep(0.1)
        return df
    
    def get_preday(self, date, n=1):
        date = pd.to_datetime(date)
        preday = date-pd.Timedelta(days=n)
        preday = preday.strftime('%Y%m%d')
        return preday
    
    def get_nextday(self, date, n=1):
        return self.get_preday(date, -n)

    # 查看两日间隔是不是交易日
    def check_is_trade_day(self, start_date, end_date):
        if (start_date not in self.trade_calendar.index) or \
            (end_date not in self.trade_calendar.index):
            self.trade_calendar = self.get_trade_calendars(start_date, end_date)
            self.trade_calendar.set_index('cal_date', inplace=True)
        between = self.trade_calendar[self.trade_calendar.index >= start_date]
        between = between[between.index < end_date]
        if between['is_open'].sum() == 0:
            return False
        return True

    # 查询交易日历
    def get_trade_calendars(self, start_date, end_date):
        self.cursor.execute('show tables like "trade_calendar"')
        result = self.cursor.fetchall()
        if not result:
            return self._get_trade_calendars(start_date, end_date)
        sql = 'select min(cal_date), max(cal_date) from %s' % 'trade_calendar'
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        min_trade_date, max_trade_date = result[0]
        if min_trade_date is None or max_trade_date is None:
            return self._get_trade_calendars(start_date, end_date)
        if start_date < min_trade_date or end_date > max_trade_date:
            return self._get_trade_calendars(start_date, end_date)
        sql = 'select * from %s ' % ('trade_calendar')
        df = pd.read_sql(sql, con=self.engine)
        return df

    # 获取交易日历
    def _get_trade_calendars(self, start_date, end_date, **kwargs):
        print('正在从tushare获取数据-->(trade_calendar, start_date: %s, end_date: %s)' % (start_date, end_date))
        trade_calendar_df = self.pro.trade_cal(start_date=start_date, end_date=end_date, **kwargs)
        sql = 'drop table if exists %s' % 'trade_calendar'
        self.cursor.execute(sql)
        trade_calendar_df.sort_values(by='cal_date')
        trade_calendar_df.to_sql('trade_calendar', con=self.engine, if_exists='append', index=False)
        return trade_calendar_df

if __name__ == '__main__':
    g = GetData('f558cbc6b24ed78c2104e209a8a8986b33ec66b7c55bcfa2f46bc108')
    df = g.get_stock_data_by_tushare('000001.SZ', '20200101', '20241013')
    print(df)