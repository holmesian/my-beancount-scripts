import re

import dateparser


def get_eating_account(from_user, description, time=None):
    if time == None or not hasattr(time, 'hour'):
        return 'Expenses:Eating:Others'
    elif time.hour <= 3 or time.hour >= 21:
        return 'Expenses:Eating:Nightingale'
    elif time.hour <= 10:
        return 'Expenses:Eating:Breakfast'
    elif time.hour <= 16:
        return 'Expenses:Eating:Lunch'
    else:
        return 'Expenses:Eating:Supper'


def get_credit_return(from_user, description, time=None):
    for key, value in credit_cards.items():
        if key == from_user:
            return value
    return "Unknown"


public_accounts = [
    'Assets:Company:Alipay:StupidAlipay'
]

credit_cards = {
    '招商银行5993': 'Liabilities:CreditCard:招商银行5993',
    '建设银行7816': 'Liabilities:CreditCard:建设银行7816',
    '平安银行0912': 'Liabilities:CreditCard:平安银行0912',
    '花呗': 'Liabilities:Company:Huabei',

}

accounts = {
    "余额宝": 'Assets:Company:Alipay:MonetaryFund',
    '余利宝': 'Assets:Bank:MyBank',
    '交通银行8447': 'Assets:DebitCard:交通银行8447',
    '招商银行9061': 'Assets:DebitCard:招商银行9061',
    '建设银行2057': 'Assets:DebitCard:建设银行2057',
    '平安银行': 'Assets:DebitCard:平安银行1940',
    '零钱': 'Assets:Balances:WeChat',
}

descriptions = {
    #'滴滴打车|滴滴快车': get_didi,
    '余额宝.*收益发放': 'Assets:Company:Alipay:MonetaryFund',
    '转入到余利宝': 'Assets:Bank:MyBank',
    '花呗收钱服务费': 'Expenses:Fee',
    '自动还款-花呗.*账单': 'Liabilities:Company:Huabei',
    '信用卡自动还款|信用卡还款': get_credit_return,
    '汽车加油': 'Liabilities:Company:中石化',
    # '外卖订单': get_eating_account,
    # '美团订单': get_eating_account,
    # '上海交通卡发行及充值': 'Expenses:Transport:Card',
    # '地铁出行': 'Expenses:Transport:City',
    # '火车票': 'Expenses:Travel:Transport',
}

anothers = {
    # '上海拉扎斯': get_eating_account
}

incomes = {
    '余额宝.*收益发放': 'Income:Trade:PnL',
}

description_res = dict([(key, re.compile(key)) for key in descriptions])
another_res = dict([(key, re.compile(key)) for key in anothers])
income_res = dict([(key, re.compile(key)) for key in incomes])
