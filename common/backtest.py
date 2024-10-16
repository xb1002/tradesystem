import pymysql
import pandas as pd

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from common.get_config import Config
from common.get_data import GetData

drop_exist_table = True
tabel_order = 'table_order'
table_portfolio = 'table_portfolio'
table_position = 'table_position'
table_signal = 'table_signal'

class Database:
    def __init__(self):
        self.config = Config()
        self.new_create_table = []
        self.mysql_config = {'mysql_host': self.config['mysql_host'], 'mysql_port': self.config['mysql_port'],
                             'mysql_username': self.config['mysql_username'], 'mysql_password': self.config['mysql_password'],
                             'database': self.config['database']}
        self.db = pymysql.connect(host=self.mysql_config['mysql_host'], user=self.mysql_config['mysql_username'],
                                password=self.mysql_config['mysql_password'], database=self.mysql_config['database'])
        self.cursor = self.db.cursor()
        if drop_exist_table:
            self.drop_exist_table()

        
    def get_cursor(self):
        return self.cursor
    
    def create_table(self, table_name, columns):
        sql = f"CREATE TABLE if not exists {table_name} ({columns})"
        self.cursor.execute(sql)
        self.db.commit()
        self.new_create_table.append(table_name)

    def drop_exist_table(self):
        self.cursor.execute("drop table if exists %s" % tabel_order)
        self.cursor.execute("drop table if exists %s" % table_portfolio)
        self.cursor.execute("drop table if exists %s" % table_position)
        self.cursor.execute("drop table if exists %s" % table_signal)
        self.db.commit()

# 订单
class Order:
    def __init__(self,db):
        db.create_table(tabel_order, 'order_id INT AUTO_INCREMENT PRIMARY KEY, order_date DATE, code VARCHAR(10), price FLOAT, amount INT, direction VARCHAR(10)')
        self.db_cursor = db.get_cursor()

    def add_order(self, date, code, price, amount, direction):
        self.date = date
        self.code = code
        self.price = price
        self.amount = amount
        self.direction = direction
        self.db_cursor.execute(f"INSERT INTO {tabel_order} (order_date, code, price, amount, direction) VALUES ('{date}', '{code}', {price}, {amount}, '{direction}')")
        self.db_cursor.connection.commit()

# 资产
class Portfolio:
    def __init__(self,db):
        self.db_cursor = db.get_cursor()
        self.portfolio = {} # code:(date, amount, market_value)
        self.position = {} # code:(date, amount, avgprice, cost)
        db.create_table(table_portfolio, 'date DATE, code VARCHAR(10), amount INT, market_price FLOAT, market_value FLOAT')
        db.create_table(table_position, 'date DATE, code VARCHAR(10), amount INT, avgprice FLOAT, cost FLOAT')
    
    def _update_portfolio(self, date, code, amount, price):
        self.db_cursor.execute(f"INSERT INTO {table_portfolio} (date, code, amount, market_price, market_value) VALUES ('{date}', '{code}', {amount}, {price}, {amount*price})")
        self.db_cursor.connection.commit()

    def _update_position(self, date, code, amount, price):
        self.db_cursor.execute(f"INSERT INTO {table_position} (date, code, amount, avgprice, cost) VALUES ('{date}', '{code}', {amount}, {price}, {amount*price})")
        self.db_cursor.connection.commit()

    def add_position(self, date, code, amount, price):
        if code in self.position:
            self.position[code]['amount'] += amount
            self.position[code]['cost'] += amount * price
            self.position[code]['avgprice'] = self.position[code]['cost'] / self.position[code]['amount']
        else:
            self.position[code] = {'amount': amount, 'avgprice': price, 'cost': amount * price}
        self._update_position(date, code, self.position[code]['amount'], self.position[code]['avgprice'])
    
    def remove_position(self, date, code, amount, price):
        if code in self.position:
            self.position[code]['amount'] -= amount
            if self.position[code]['amount'] == 0:
                del self.position[code]
                self._update_position(date, code, 0, price)
                return
            self.position[code]['cost'] -= amount * price
            self.position[code]['avgprice'] = self.position[code]['cost'] / (self.position[code]['amount']+1e-6)
            self._update_position(date, code, self.position[code]['amount'], self.position[code]['avgprice'])
        else:
            print('No such position')
    
    def update_portfolio(self, date, code, amount, price):
        if code in self.portfolio:
            self.portfolio[code]['amount'] += amount
            self.portfolio[code]['market_value'] = self.portfolio[code]['amount'] * price
        else:
            self.portfolio[code] = {'amount': amount, 'market_value': amount * price}
        self._update_portfolio(date, code, self.portfolio[code]['amount'], price)

# 账户
class Account:
    def __init__(self):
        self.cash = 0
        self.total_deposit = 0
        self.init_db()
        self.portfolio = Portfolio(self.db)
        self.order = Order(self.db)
    
    def init_db(self):
        self.db = Database()

    def add_cash(self, cash):
        self.cash += cash
        self.total_deposit += cash

    def buy(self, date, code, price, amount):
        if self.cash < price * amount:
            print('Insufficient funds')
            return
        self.cash -= price * amount
        self.order.add_order(date, code, price, amount, 'buy')
        self.portfolio.add_position(date, code, amount, price)
        self.portfolio.update_portfolio(date, code, amount, price)

    def sell(self, date, code, price, amount):
        if code not in self.portfolio.position.keys():
            print('No such position')
            return
        if self.portfolio.position[code]['amount'] < amount:
            print('Insufficient position')
            return
        self.cash += price * amount
        self.portfolio.remove_position(date, code, amount, price)
        self.portfolio.update_portfolio(date, code, -amount, price)
        self.order.add_order(date, code, price, amount, 'sell')

    def place_order(self, date, code, price, amount, direction):    # direction: 'buy' or 'sell'
        if direction == 'buy':
            self.buy(date, code, price, amount)
        elif direction == 'sell':
            self.sell(date, code, price, amount)

    def everyday_portfolio_update(self, date, code, price):
        if code in self.portfolio.portfolio.keys():
            self.portfolio.update_portfolio(date, code, 0, price)

class Signal:
    def __init__(self,db):
        self.signal = pd.DataFrame(columns=['code', 'date', 'sig', 'price'])
        db.create_table(table_signal, 'code VARCHAR(10), date DATE, sig VARCHAR(10), price FLOAT')
        self.db_cursor = db.get_cursor()

    def _add_signal(self, code, date, sig, price):
        self.db_cursor.execute(f"INSERT INTO {table_signal} (code, date, sig, price) VALUES ('{code}', '{date}', '{sig}', {price})")
        self.db_cursor.connection.commit()

    def add_signal(self, code, date, sig, price):
        self._add_signal(code, date, sig, price)
        sig = pd.DataFrame({'code': [code], 'date': [date], 'sig': [sig], 'price': [price]})
        self.signal = pd.concat([self.signal, sig], ignore_index=True)

# 评价指标
class Metrics:
    pass

# 回测引擎
class Backtest:
    def __init__(self, account:Account, data):
        self.account = account
        self.data = data

    def run(self, signals, code): # 这里交易信号为DataFrame,每次只能回测一只股票
        signal_dates = signals['date'].tolist()
        close_prices = self.data['close']
        could_sell_amount = 0
        total_value = []
        for i in range(len(self.data)):
            date = self.data.iloc[i]['trade_date']
            close_price = close_prices.iloc[i]
            if date in signal_dates:
                sig = signals[signals['date'] == date]['sig'].values[0]
                price = signals[signals['date'] == date]['price'].values[0]
                if sig == 'buy':
                    could_buy_amount = int(self.account.cash / price)
                    self.account.buy(date, code, price, could_buy_amount)
                    could_sell_amount = could_buy_amount
                elif sig == 'sell':
                    self.account.sell(date, code, price, could_sell_amount)
                    could_sell_amount = 0
            self.account.everyday_portfolio_update(date, code, close_price)
            if code in self.account.portfolio.position.keys():
                total_value.append(self.account.cash + self.account.portfolio.portfolio[code]['market_value'])
            else:
                total_value.append(self.account.cash)
        
        # 查看余额
        cursor = self.account.db.get_cursor()
        # 查看portfolio与position最后一天的情况
        cursor.execute(f"SELECT * FROM {table_position} WHERE date=(SELECT MAX(date) FROM {table_position})")
        result = cursor.fetchall()
        print(f"position: {result}")
        cursor.execute(f"SELECT * FROM {table_portfolio} WHERE date=(SELECT MAX(date) FROM {table_portfolio})")
        result = cursor.fetchall()
        print(f"portfolio: {result}")
        print(f"cash: {self.account.cash}")
        print(f"total: {self.account.cash+result[0][-1]}")
        return total_value

# 策略/交易信号
class Strategy:
    def __init__(self, data, db):
        self.sigs = Signal(db)
        self.data = data

    def cross_strategy(self, short=5, long=10):
        self.data['short'] = self.data['close'].rolling(short).mean()
        self.data['long'] = self.data['close'].rolling(long).mean()
        self.data['sig'] = 0
        self.data.loc[self.data['short'] > self.data['long'], 'sig'] = 1
        self.data.loc[self.data['short'] < self.data['long'], 'sig'] = -1
        self.data.dropna(inplace=True)
        self.data['pre_sig'] = self.data['sig'].shift(1)
        self.data['result_sig'] = self.data.apply(lambda x: 'buy' if x['sig'] > x['pre_sig'] \
                                                    else 'sell' if x['sig'] < x['pre_sig'] else 'wait', axis=1)
        temp_sig = self.data[self.data['result_sig'] != 'wait']
        for i in range(len(temp_sig)):
            sig = temp_sig.iloc[i]
            self.sigs.add_signal(sig['ts_code'], sig['trade_date'], sig['result_sig'], sig['close'])
        return self.sigs.signal

if __name__ == '__main__':
    code = '000158.SZ'
    start_date = '20200101'
    end_date = '20231013'
    g = GetData('f558cbc6b24ed78c2104e209a8a8986b33ec66b7c55bcfa2f46bc108')
    df = g.get_stock_data_by_tushare(code, start_date, end_date,adj='qfq')
    account = Account()
    account.add_cash(100000)   # 初始化账户资金
    bt = Backtest(account, df)
    s = Strategy(df, account.db)
    sig = s.cross_strategy()
    total_value = bt.run(sig, code)
    import matplotlib.pyplot as plt
    plt.plot(total_value)
    plt.show()