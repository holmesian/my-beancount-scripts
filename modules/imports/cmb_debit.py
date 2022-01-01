import re
from datetime import date, datetime
from io import StringIO

from beancount.core import data
from beancount.core.data import Transaction

from ..accounts import accounts
from . import (DictReaderStrip, get_account_by_guess,
               get_income_account_by_guess, replace_flag)
from .base import Base
from .deduplicate import Deduplicate

# 储蓄卡/借记卡账户
AccountCMBDebit = 'Assets:BALANCE:Card:CMB'
# 理财账户
AccountCMBFinancial = 'Assets:FINANCIAL:Fund:CMB'


# 仅支持借记卡一网通导入，信用卡还是按照原来的方式
class CMBDebit(Base):
    def __init__(self, filename, byte_content, entries, option_map):
        if re.search(r'CMB_.*\.csv$', filename):
            content = byte_content.decode("gbk")
            lines = content.split("\n")
            tName = lines[0].replace(',', '').replace('#', '').strip()
            if (tName != '招商银行交易记录'):
                print(tName)
                raise 'Not CMB Trade Record!'
        print('Import CMB: ' +
              lines[2].replace(',', '').replace('#', '').strip())
        content = "\n".join(lines[7:len(lines)])
        self.content = content
        self.deduplicate = Deduplicate(entries, option_map)

    def parse(self):
        content = self.content
        f = StringIO(content)
        reader = DictReaderStrip(f, delimiter=',')
        transactions = []
        for row in reader:
            print("Importing {} at {}".format(row['交易类型'], row['交易日期']))
            if row['交易时间'] == '':
                continue
            meta = {}
            time = datetime.strptime((row['交易日期'] + ' ' + row['交易时间']),
                                     '%Y%m%d %H:%M:%S')
            meta['trade_time'] = datetime.strftime(time, '%Y-%m-%d %H:%M:%S')
            meta['timestamp'] = str(time.timestamp()).replace('.0', '')
            account = get_account_by_guess(row['交易备注'], row['交易类型'], time)
            flag = "*"
            meta = data.new_metadata('beancount/core/testing.beancount', 12345,
                                     meta)
            entry = Transaction(meta, date(time.year, time.month, time.day),
                                flag, row['交易备注'], row['交易类型'], data.EMPTY_SET,
                                data.EMPTY_SET, [])

            # 获取当前不为空的row['收入'] row['支出]
            ways = self.getCurrentWays(row['收入'], row['支出'])
            amount_string = ''
            amount = 0

            if ways == True:
                amount_string = row['收入']
                amount = float(amount_string)
                if '朝朝宝转出' in row['交易类型']:
                    # 因为招行记录是记了两遍的，所以这里直接默认是赎回，然后统一在下方处理成了银行卡支付
                    entry = entry._replace(narration=row['交易类型'])
                    data.create_simple_posting(entry, AccountCMBFinancial,
                                               None, 'CNY')
                else:
                    income = get_income_account_by_guess(
                        row['交易备注'], row['交易类型'], time)
                    if income == 'Income:Unknown':
                        entry = replace_flag(entry, '!')
                    data.create_simple_posting(entry, income, None, 'CNY')
                data.create_simple_posting(entry, AccountCMBDebit,
                                           amount_string, 'CNY')
            elif ways == False:
                amount_string = row['支出']
                amount = float(amount_string)
                if '朝朝宝购买' in row['交易类型']:
                    entry = entry._replace(payee='')
                    entry = entry._replace(narration='购买朝朝宝')
                    data.create_simple_posting(entry, AccountCMBFinancial,
                                               amount_string, 'CNY')
                else:
                    account = get_account_by_guess(row['交易备注'], row['交易类型'],
                                                   time)
                    if account == "Expenses:Unknown":
                        entry = replace_flag(entry, '!')
                    data.create_simple_posting(entry, account, amount_string,
                                               'CNY')
                data.create_simple_posting(entry, AccountCMBDebit, None, None)
            else:
                print('Unknown row', row)
            if not self.deduplicate.find_duplicate(entry, -amount, None,
                                                   AccountCMBDebit):
                transactions.append(entry)

        self.deduplicate.apply_beans()
        return transactions

    def getCurrentWays(self, income, pointout):
        if income == '':
            return False
        elif pointout == '':
            return True
        else:
            return None
