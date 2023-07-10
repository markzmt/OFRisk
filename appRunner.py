# -*- coding: utf-8 -*-
# Create time   : 2023-06-28
# Author        : ZhanYin
# File name     : util.py
# Project name  : OFRisk

from datetime import datetime, date
import sqlite3 as sl
import sql_text as st
import config_Text as ct
import pandas as pd
from iFinDPy import *
import numpy as np
import uuid
import click
import pathlib

array = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
         "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v",
         "w", "x", "y", "z",
         "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V",
         "W", "X", "Y", "Z"
         ]
CallPut = {'看跌期权': 'C', '看涨期权': 'P'}
commodityPattern = re.compile(r'^[A-Za-z]{1,2}')
instrumentPattern = re.compile(r'^[A-Za-z0-9]{4,6}')
callputPattern = re.compile(r'[A-Za-z]{1,2}')
DB = 'D:/OFRisk/OFRisk.db'
workPath = "D:/log/YYYYMMDD/"
fileTypes = ['融行日结算单', '市场监控中心日结算单', '东证期货结算单', '期现销购', '镒链日终', '飞豹']
accounts = ['201888', '201913', '88760378', '88760665']
option_account = '88760665'
def toNextDate(date):
    if int(str(date)[-2:]) >= 31:
        return date + 70
    else:
        return date + 1
def isNan(input):
    # print(input, type(input))
    if not isinstance(input, float) and not isinstance(input, int):
        return 0
    elif np.isnan(input):
        return 0
    elif isinstance(input, float):
        return float(input)
    elif isinstance(input, int):
        return int(input)
def isStrNan(input):
    if isinstance(input, str)  or isinstance(input, date):
        return input
    else:
        return ""
def timestamp2Str(input):
    return datetime.fromtimestamp(input).strftime("%Y-%m-%d %H:%M:%S")
def getIndexPrice(indexName):
    if 'IC' in indexName:
        return 'IC'
    elif 'IF' in indexName or 'IO' in indexName:
        return 'IF'
    elif 'IM' in indexName or 'MO' in indexName:
        return 'IM'
    else:
        return 'IH'
def returnValid(keyWord, dataFrame):
    for sentence in dataFrame.itertuples(index=False):
        if keyWord in sentence:
            if sentence[sentence.index(keyWord) + 1] == 'nan':
                return sentence[sentence.index(keyWord) + 1]
            else:
                return sentence[sentence.index(keyWord) + 2]
        else:
            continue
    return ""


def findKeywords(keyWords, dataFrame, mode):
    if mode == 1:
        for sentence in dataFrame.itertuples():
            if keyWords in sentence:
                return sentence.Index
    elif mode == 2:
        for sentence in dataFrame.itertuples():
            if keyWords in sentence[1]:
                return sentence.Index
    return 0


def getValueDict(targetList, dataFrame, function):
    result = {}
    for item in targetList:
        result[item] = function(item, dataFrame)
    return result


def listdir(path):
    fileList = []
    for home, dirs, files in os.walk(path):
        for filename in files:
            fileList.append(os.path.join(home, filename))
    return fileList


def hasSeparator(productName):
    if '_' in productName:
        return productName.split('_')[0] + '","' + productName.split('_')[1]
    else:
        return productName + '","o'


def getPositionDetail(targetList, dataFrame, file_type):
    if file_type == '融行日结算单':
        futures = {}
        options = {}
        for item in targetList:
            pos_target = findKeywords(item, dataFrame, 1)
            if pos_target == 0:
                break
            sub_df = dataFrame[pos_target + 2:]
            for sentence in sub_df.itertuples():
                # print(sentence)
                if '合计' in sentence:
                    break
                elif '-' not in sentence[3]:
                    if sentence[3].upper() not in futures:
                        futures[sentence[3].upper()] = [isNan(sentence[4]), isNan(sentence[6])]
                    else:
                        futures[sentence[3].upper()] = list(map(lambda x, y: x + y, futures[sentence[3].upper()],
                                                                [isNan(sentence[4]), isNan(sentence[6])]))
                else:
                    if sentence[3].upper() not in options:
                        options[sentence[3].upper()] = [isNan(sentence[4]), isNan(sentence[6]), isNan(sentence[16]),
                                                        isNan(sentence[17])]
        return futures, options
    elif file_type == '市场监控中心日结算单':
        futures = {}
        options = {}
        for item in targetList:
            pos_target = findKeywords(item, dataFrame, 1)
            if pos_target == 0:
                break
            # print(dataFrame[pos_target:])
            sub_df = dataFrame[pos_target + 2:]
            for sentence in sub_df.itertuples():
                if '合计' in sentence:
                    break
                elif '期货' in item:
                    if sentence[1] not in futures:
                        futures[sentence[1]] = [isNan(sentence[2]), isNan(sentence[4])]
                    else:
                        futures[sentence[1]] = list(
                            map(lambda x, y: x + y, futures[sentence[1]], [isNan(sentence[2]), isNan(sentence[4])]))
                else:
                    if sentence[1] + '.' + sentence[2] + '.' + sentence[3] not in options:
                        options[sentence[1] + '.' + sentence[2] + '.' + sentence[3]] = [isNan(sentence[5]),
                                                                                        isNan(sentence[7])]
                    else:
                        options[sentence[1] + '.' + sentence[2] + '.' + sentence[3]] = list(
                            map(lambda x, y: x + y, options[sentence[1] + '.' + sentence[2] + '.' + sentence[3]],
                                [isNan(sentence[5]), isNan(sentence[7])]))
        return futures, options
    elif file_type == '东证期货结算单':
        option = {}
        for item in targetList:
            pos_target = findKeywords(item, dataFrame, 2)
            # print("find the target line: ", pos_target)
            if pos_target == 0:
                break
            sub_df = dataFrame[pos_target + 5:]
            for sentence in sub_df.itertuples(index=False):
                if '-----------' in sentence[0]:
                    break
                elif '期权' in sentence[0]:
                    # print(sentence[0].split('|')[4],sentence[0].split('|')[1],sentence[0].split('|')[14],sentence[0].split('|')[15])
                    option[sentence[0].split('|')[4].strip().upper()] = [sentence[0].split('|')[14].strip(),
                                                                         sentence[0].split('|')[15].strip(),
                                                                         sentence[0].split('|')[1].strip()]
        return option


def get_short_id():
    id = str(uuid.uuid4()).replace("-", '')  # 注意这里需要用uuid4
    buffer = []
    for i in range(0, 8):
        start = i * 4
        end = i * 4 + 4
        val = int(id[start:end], 16)
        buffer.append(array[val % 62])
    return "".join(buffer)


def getInstrumentList(instruments):
    commodity = "'"
    index = []
    for instrment in instruments:
        if commodityPattern.match(instrment[0]).group(0).lower() in ct.exchangeList:
            commodity += (instrment[0] + "." + ct.exchangeList[commodityPattern.match(instrment[0]).group(0).lower()])
            commodity += ","
            # print(instrment[0] + "." + ct.exchangeList[commodityPattern.match(instrment[0]).group(0).lower()])
        else:
            index.append(instrment[0])
            # print(instrment[0])
    commodity = commodity[:-1]
    commodity += "'"
    return commodity, index


def getIndexMultiplier(indexName):
    if 'O' in indexName:
        return 100
    elif 'IC' in indexName:
        return 200
    elif 'IF' in indexName:
        return 300
    elif 'IM' in indexName:
        return 200
    else:
        return 300

class FIFO:
    def __init__(self, dataBase, report_date, month_end):
        self.history = {}
        self.profit = {}
        self.dataBase = dataBase
        self.report_date = report_date
        self.month_end = month_end
        self.load_inventory()
    def isCommodity(self, input, commodityList):
        for item in commodityList:
            #print(item, input)
            if input in item:
                return 1
            else:
                return 0
    def load_inventory(self):
        connection = sl.connect(self.dataBase)
        cn = connection.cursor()
        commodityList = cn.execute(st.commodityListStr).fetchall()
        print("list:", commodityList)
        folder_name = workPath.replace('YYYYMMDD', str(self.month_end) + '/' + '存货汇总')
        if os.path.exists(folder_name):
            print("Processing folder 存货汇总 for date: ", self.month_end)
            for fileName in listdir(folder_name):
                print("Processing file: ", fileName)
                df = pd.read_excel(fileName,sheet_name=[0])
                #for item in df.values():
                    #for line in item.itertuples():
                        #commodityName = str(line[1]).split('（')[0]
                        #print(commodityName)
                        #if self.isCommodity(commodityName, commodityList):
                            #print(commodityName)

    def calculate_fifo_pnl(self):
        connection = sl.connect(self.dataBase)
        cn = connection.cursor()
        history = cn.execute(st.fifoPnLStr.format('2023', '05')).fetchall()
        #for line in history:
            #print(line)

class DataProcess:
    def __init__(self, dataBase):
        self.dataBase = dataBase
        self.init_db()
        self.thslogindemo("dzrh010", "b28f20")

    def thslogindemo(self, user, psw):
        # 输入用户的帐号和密码
        thsLogin = THS_iFinDLogin(user, psw)
        print(thsLogin)
        if thsLogin != 0:
            print('failed to login')
        else:
            print('successful to login')

    def init_db(self):
        file_name = self.dataBase
        connection = sl.connect(file_name)
        tableCreation = connection.cursor()
        connection.commit()
        tableCreation.close()
        connection.close()
        return file_name

    def init_table(self):
        file_name = self.dataBase
        connection = sl.connect(file_name)
        tableCreation = connection.cursor()
        # Drop table if exist
        for sql in st.genDropTable(st.tables):
            tableCreation.execute(sql)
        # Create table as definitions
        for sql in st.Definitions:
            tableCreation.execute(sql)
        connection.commit()
        tableCreation.close()
        # 关闭连接
        connection.close()
        return file_name

    def delete_table(self, date):
        file_name = self.dataBase
        connection = sl.connect(file_name)
        tableDelete = connection.cursor()
        for table in st.tables:
            #print("deleting table: ", st.DeleteByDateStr.format(table, date))
            try:
                tableDelete.execute(st.DeleteByDateStr.format(table, date))
            except Exception as sqlError:
                #print(sqlError)
                pass
            continue
        connection.commit()
        tableDelete.close()
        connection.close()
    def insertMarketData(self, DB, date, commodityList, indexList):
        indexTable = {}
        price_df = pd.DataFrame()
        index_df = pd.DataFrame()
        indexPrices = THS_BD('000016.SH,000300.SH,000852.SH,000905.SH', 'ths_close_price_stock', date)
        prices = THS_BD(commodityList[1:-1],
                        'ths_settle_future;ths_close_price_future;ths_contract_multiplier_future;ths_vol_future;ths_open_interest_future',
                        date + ';' + date + ';;' + date + ';' + date)
        price_df = pd.concat([price_df, prices.data])
        # print(pd.concat([index_df, indexPrices.data]))
        for item in pd.concat([index_df, indexPrices.data]).itertuples():
            if '000016.SH' in item[1]:
                indexTable['IH'] = item[2]
            elif '000300.SH' in item[1]:
                indexTable['IF'] = item[2]
            elif '000852.SH' in item[1]:
                indexTable['IM'] = item[2]
            elif '000905.SH' in item[1]:
                indexTable['IC'] = item[2]
        try:
            connection = sl.connect(DB.replace('YYYYMMDD', date))
            insertData = connection.cursor()
            for item in price_df.itertuples():
                strData = '"' + item[1].split('.')[0] + '",' + date + ',' + str(isNan(item[2])) + ',' + str(
                    isNan(item[3])) + ',' + str(isNan(item[4])) + ',' + str(isNan(item[5])) + ',' + str(isNan(item[6]))
                # print(st.InsertDataStr.format('marketData', strData))
                insertData.execute(st.InsertDataStr.format('marketData', strData))
            for item in indexList:
                strData = '"' + item + '",' + date + ',' + str(indexTable[getIndexPrice(item)]) + ',0,' + str(
                    getIndexMultiplier(item)) + ',0,0'
                # print(strData)
                insertData.execute(st.InsertDataStr.format('marketData', strData))
            connection.commit()
            insertData.close()
            connection.close()
        except Exception as sqlError:
            print(sqlError)
            pass

    def process_file(self, file, file_type, connection, date):
        futures = {}
        options = {}
        pnl = {}
        commodity = {}
        cn = connection.cursor()
        pnl_words = ['账户', '客户期货期权内部资金账户', '交易日期', '上日结存', '当日结存', '客户权益', '平仓盈亏',
                     '浮动盈亏', '当日手续费', '保证金占用', '当日存取合计']
        if file_type == '融行日结算单':
            df = pd.read_excel(file)
            pnlDetail = getValueDict(pnl_words, df, returnValid)
            strData = '"' + str(pnlDetail['账户']) + '",' + str(pnlDetail['交易日期']) + ',' + str(
                pnlDetail['上日结存']) + ',' + str(pnlDetail['当日结存']) + ',' + str(
                pnlDetail['客户权益']) + ',' + str(
                pnlDetail['平仓盈亏']) + ',' + str(pnlDetail['浮动盈亏']) + ',' + str(
                pnlDetail['当日手续费']) + ',' + str(pnlDetail['保证金占用']) + ',' + str(pnlDetail['当日存取合计'])
            cn.execute(st.InsertDataStr.format('settlement', strData))
            futures, options = getPositionDetail(["持仓汇总"], df, file_type)
            for future in futures:
                strFuture = '"' + future + '",' + str(pnlDetail['交易日期']) + ',"' + commodityPattern.match(
                    future).group(
                    0) + '",' + str(pnlDetail['账户']) + ',' + str(futures[future][0]) + ', 0, ' + str(
                    futures[future][1]) + ', 0'
                # print(st.InsertDataStr.format('future', strFuture))
                cn.execute(st.InsertDataStr.format('future', strFuture))
            for option in options:
                strOption = '"' + option + '","' + instrumentPattern.match(option).group(
                    0) + '",' + str(pnlDetail['交易日期']) + ',"' + callputPattern.match(option).group(0) + '","' + \
                            callputPattern.findall(option)[1] + '",' + str(pnlDetail['账户']) + ',' + str(
                    options[option][0]) + ',0,' + str(options[option][1]) + ',0,' + str(options[option][2]) + ',' + str(
                    options[option][3])
                # print(st.InsertDataStr.format('option', strOption))
                cn.execute(st.InsertDataStr.format('option', strOption))
            connection.commit()
            cn.close()
        elif file_type == '市场监控中心日结算单':
            df = pd.read_excel(file)
            pnlDetail = getValueDict(pnl_words, df, returnValid)
            pnlDetail['交易日期'] = pnlDetail['交易日期'].replace('-', '')
            strData = '"' + str(pnlDetail['客户期货期权内部资金账户']) + '",' + str(pnlDetail['交易日期']) + ',' + str(
                pnlDetail['上日结存']) + ',' + str(pnlDetail['当日结存']) + ',' + str(
                pnlDetail['客户权益']) + ',' + str(
                pnlDetail['平仓盈亏']) + ',' + str(pnlDetail['浮动盈亏']) + ',' + str(
                pnlDetail['当日手续费']) + ',' + str(pnlDetail['保证金占用']) + ',' + str(pnlDetail['当日存取合计'])
            # print(st.InsertDataStr.format('settlement', strData))
            cn.execute(st.InsertDataStr.format('settlement', strData))
            # insert position data
            position_words = ['期货持仓汇总', '期权持仓汇总']
            futures, options = getPositionDetail(position_words, df, file_type)
            # print(options)
            for future in futures:
                strFuture = '"' + future + '",' + str(pnlDetail['交易日期']) + ',"' + commodityPattern.match(
                    future).group(0) + '",' + str(pnlDetail['客户期货期权内部资金账户']) + ',' + str(
                    futures[future][0]) + ', 0, ' + str(futures[future][1]) + ', 0'
                # print(st.InsertDataStr.format('future', strFuture))
                cn.execute(st.InsertDataStr.format('future', strFuture))
            for option in options:
                strOption = '"' + option.split('.')[0] + '", "' + option.split('.')[1] + '",' + str(
                    pnlDetail['交易日期']) + ',"' + commodityPattern.match(option).group(0) + '","' + CallPut[
                                option.split('.')[2]] + '",' + str(pnlDetail['客户期货期权内部资金账户']) + ',' + str(
                    options[option][0]) + ', 0, ' + str(options[option][1]) + ', 0, 0, 0'
                # print(strOption)
                cn.execute(st.InsertDataStr.format('option', strOption))
            connection.commit()
            cn.close()
        elif file_type == '东证期货结算单':
            df = pd.read_csv(file, header=None, lineterminator="\n")
            # print(value_df)
            options = getPositionDetail(["持仓汇总"], df, '东证期货结算单')
            for option in options:
                # print(st.updateOptionValueStr.format(options[option][0],options[option][1], option, date))
                cn.execute(st.updateOptionValueStr.format(options[option][0], options[option][1], option, date,
                                                          options[option][2]))
            connection.commit()
            cn.close()
        elif file_type == '飞豹':
            greeks_df = pd.read_csv(file)
            greeks_df.replace("nan", np.nan, inplace=True)
            greeks_df = greeks_df.dropna(subset='总CashDelta', how='any')
            header = greeks_df.columns.values.tolist()
            for data in greeks_df.itertuples(index=False):
                strData = '"' + data[header.index('产品')] + '",' + str(date) + ',"' + hasSeparator(
                    data[header.index('产品')]) + '",' + str(data[header.index('总CashDelta')]) + ',' + str(
                    data[header.index('总CashGamma')]) + ',' + str(data[header.index('总PositionVega')]) + ',' + str(
                    data[header.index('总PositionTheta')])
                # print(st.InsertDataStr.format(strData))
                cn.execute(st.InsertDataStr.format('greeks', strData))
            connection.commit()
            cn.close()
        elif file_type == '期现销购':
            df = pd.read_excel(file)
            header = df.columns.values.tolist()
            # print(header)
            if '提单出库' in header:
                for data in df.itertuples(index=False):
                    # print(type(pd.to_datetime(data[header.index('合同用印日期')])))
                    strData = '"' + isStrNan(data[header.index('商品大类')]) + '", "' + isStrNan(
                        data[header.index('商品名称')]) + '", "' + str(
                        isStrNan(data[header.index('合同用印日期')].date())) + '",' + str(
                        isNan(data[header.index('合同数量')])) + ', ' + str(
                        isNan(data[header.index('合同单价')])) + ', ' + str(
                        isNan(data[header.index('结算单价')])) + ', ' + str(
                        isNan(data[header.index('结算重量')])) + ', ' + str(
                        isNan(data[header.index('提单出库')])) + ', ' + str(
                        isNan(data[header.index('实际出库')])) + ', "' + str(
                        isStrNan(data[header.index('出库时间')].date())) + '", "' + str(
                        isStrNan(data[header.index('客户')])) + '", "' + str(
                        isStrNan(data[header.index('合同号')])) + '", "' + str(
                        isStrNan(data[header.index('收款日期')].date())) + '", ' + str(
                        isNan(data[header.index('收款金额')])) + ', ' + str(
                        isNan(data[header.index('已收保证金')])) + ', "' + str(
                        isStrNan(data[header.index('业务类型')])) + '","out",' + str(date) +',"' + get_short_id() + '"'
                    #print(strData)
                    #print(type(data[header.index('合同用印日期')].date()))
                    cn.execute(st.InsertDataStr.format('commodity', strData))
            elif '提单入库' in header:
                for data in df.itertuples(index=False):
                    # print(type(data[header.index('付款金额')]))
                    strData = '"' + isStrNan(data[header.index('商品大类')]) + '", "' + isStrNan(
                        data[header.index('商品名称')]) + '", "' + str(
                        isStrNan(data[header.index('合同用印日期')].date())) + '",' + str(
                        isNan(data[header.index('合同数量')])) + ', ' + str(
                        isNan(data[header.index('合同单价')])) + ', ' + str(
                        isNan(data[header.index('结算单价')])) + ', ' + str(
                        isNan(data[header.index('结算重量')])) + ', ' + str(
                        isNan(data[header.index('提单入库')])) + ', ' + str(
                        isNan(data[header.index('实际入库')])) + ', "' + str(
                        isStrNan(data[header.index('入库时间')].date())) + '", "' + str(
                        isStrNan(data[header.index('供货商')])) + '", "' + str(
                        isStrNan(data[header.index('合同号')])) + '", "' + str(
                        isStrNan(data[header.index('付款日期')].date())) + '", ' + str(
                        isNan(data[header.index('付款金额')])) + ', ' + str(
                        isNan(data[header.index('已收保证金')])) + ', "' + str(
                        isStrNan(data[header.index('业务类型')])) + '","in",' + str(date) + ',"' + get_short_id() + '"'
                    cn.execute(st.InsertDataStr.format('commodity', strData))
                    #print(strData)
            connection.commit()
            cn.close()


    def file2database(self, date, fileTypes):
        connection = sl.connect(self.dataBase)
        cn_cursor = connection.cursor()
        # delete date by date firstly
        cn_cursor.execute(st.DeleteByDateStr.format('settlement', date))
        cn_cursor.execute(st.DeleteByDateStr.format('future', date))
        cn_cursor.execute(st.DeleteByDateStr.format('option', date))
        connection.commit()
        cn_cursor.close()
        for file_type in fileTypes:
            folder_name = workPath.replace('YYYYMMDD', str(date) + '/' + file_type)
            if os.path.exists(folder_name):
                print("Processing folder for date: ", file_type, date)
                for fileName in listdir(folder_name):
                    print("Processing file: ", fileName)
                    self.process_file(fileName, file_type, connection, date)

        connection.close()

    def update2database(self, date):
        connection = sl.connect(self.dataBase)
        cn = connection.cursor()
        instruments = cn.execute(st.getInstrumentsStr.format('future', 'option', date)).fetchall()
        commodity, index = getInstrumentList(instruments)
        self.insertMarketData(self.dataBase, date, commodity, index)
        # print(queryData.execute(st.testFutureStr.format(date)).fetchall())
        cn.execute(st.updateFutureStr.format(date))
        # update option notional value
        cn.execute(st.updateOptionStr.format(date))
        connection.commit()
        cn.close()
        connection.close()

    def genProfitLoss(self, DB, month_end, report_date, manual_insert_me=0):
        connection = sl.connect(DB)
        cn = connection.cursor()
        last_eod = cn.execute(st.lastEODStr.format(report_date)).fetchall()
        cn.execute(st.addOptionValueStr.format(option_account,report_date))
        #print(st.addOptionValueStr.format(option_account,report_date))
        connection.commit()
        #print(isNan(last_eod[0][0]))
        dtd_pnl = cn.execute(st.DTDPnlStr.format(report_date, isNan(last_eod[0][0]))).fetchall()
        #print(st.DTDPnlStr.format(report_date, isNan(last_eod[0][0])))
        #print(dtd_pnl)
        for line in dtd_pnl:
            sqlStr = str(line[1]) + ',' + str(line[0]) + ','+ str(month_end) + ',0,0,'+ str(line[2]) + ',0'
            cn.execute(st.InsertDataStr.format('profitLoss', sqlStr))
        connection.commit()
        if manual_insert_me:
            for acc, pnl in zip(accounts, ct.ytd_pnl):
                cn.execute(st.ytdStr.format(pnl,str(month_end),acc))
        cn.execute(st.MTDPnlStr.format(report_date))
        connection.commit()
        cn.close()
        connection.close()

@click.command()
@click.option("-init", "--init", type=int, help="input 1 to init DB, 0 to skip init")
@click.option("-filetypes", "--filetypes", type=str,
              help="融行日结算单, 市场监控中心日结算单, 东证期货结算单, 飞豹, 期现销购, 镒链日终")
@click.option("-db", "--db", type=str, help="DB path like: D:/OFRisk/OFRisk.db")
@click.option("-str_date", "--str_date", type=int, help="load data from str_date")
@click.option("-end_date", "--end_date", type=int, help="load data to end_date")
@click.option("-report_date", "--report_date", type=int, help="report by date")
@click.option("-month_end", "--month_end", type=int, help="last month end")
@click.option("-manual_insert_me", "--manual_insert_me", type=int, help="manual insert tillME")
def main(init, filetypes, db, str_date, end_date, report_date, month_end, manual_insert_me):
    filetypes = eval(filetypes)
    # DB = 'D:/OFRisk/MarketMaker.db'
    print("parameter: ", init, filetypes, db, str_date, end_date)
    dp = DataProcess(db)
    if init:
        dp.init_table()
    eod_date = str_date
    while (eod_date <= end_date):
        dp.delete_table(eod_date)
        dp.file2database(eod_date, filetypes)
        dp.update2database(str(eod_date))
        dp.genProfitLoss(db, month_end, eod_date, manual_insert_me)
        #move date to the next
        eod_date = toNextDate(eod_date)
        for folder in fileTypes:
            pathlib.Path(workPath.replace('YYYYMMDD', str(eod_date) + '/' + folder)).mkdir(parents=True, exist_ok=True)

    #fifo_pnl = FIFO(db, report_date, month_end)
    #fifo_pnl.calculate_fifo_pnl()

if __name__ == '__main__':
    main()

