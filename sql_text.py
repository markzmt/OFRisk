
indexPrices = ['3833.6','6513.2']
multipliers = ['100', '100']
indexList = [['IO2306', 'IO2307', 'IO2308', 'IO2309', 'IO2310', 'IO2311', 'IO2312', 'IO2401', 'IO2402', 'IO2403'],['MO2306', 'MO2307', 'MO2308', 'MO2309', 'MO2310', 'MO2311', 'MO2312', 'MO2401', 'MO2402', 'MO2403']]
tables = ['inventory', 'commodity', 'marketData', 'profitLoss', 'future', 'option', 'greeks', 'settlement']
InsertDataStr = '''INSERT INTO {0} VALUES ({1});'''
DeleteByDateStr = '''DELETE FROM {0} WHERE date = {1};'''
updateFutureStr = "replace into future select f.instrument,f.date,f.commodity,f.account,f.long_quantity,f.long_quantity*m.settlementPrice*m.multiplier_f,f.short_quantity,f.short_quantity*m.settlementPrice*m.multiplier_f from future f left join marketData m on f.instrument=m.instrument and f.date=m.date where f.date = {0}"
updateOptionStr = "replace into option select o.contract,o.instrument,o.date,o.commodity,o.callPut,o.account,o.long_quantity,o.long_quantity*m.settlementPrice*m.multiplier_f,o.short_quantity,o.short_quantity*m.settlementPrice*m.multiplier_f,o.long_value,o.short_value from option o left join marketData m on o.instrument=m.instrument and o.date=m.date where o.date = {0}"
getInstrumentsStr = "select distinct instrument from {0} where date = {2} UNION SELECT distinct instrument FROM {1} where date = {2}"
testFutureStr = "select f.instrument,f.date,f.commodity,f.account,f.long_quantity,f.long_quantity*m.settlementPrice*m.multiplier_f,f.short_quantity,f.short_quantity*m.settlementPrice*m.multiplier_f from future f left join marketData m on f.instrument=m.instrument and f.date=m.date where f.date = {0}"
updateOptionValueStr = "replace into option select contract,instrument,date,commodity,callPut,account,long_quantity,long_notional,short_quantity,short_notional,{0},{1} from option where contract = '{2}' and date={3} and account like '%{4}%'"
lastEODStr = "select max(date) as lastDate from settlement where date <{0}"
DTDPnlStr = "select t1.date, t1.account, t1.equity-t2.equity-t1.deposit+t1.return as dtd_pnl from settlement t1 join settlement t2 on t1.account=t2.account where t1.date={0} and t2.date={1}"
MTDPnlStr = "replace into profitLoss select p1.account, p2.date, p2.monthEnd,p3.ytd_pnl+sum(p1.dtd_pnl) as ytd_pnl,sum(p1.dtd_pnl) as mtd_pnl, p2.dtd_pnl, p3.ytd_pnl from profitLoss p1 join profitLoss p2 on p1.account=p2.account join profitLoss p3 on p1.account=p3.account where p1.date > p1.monthEnd and p1.date <={0} and p2.date={0} and p3.date=p1.monthEnd group by p1.account"
ytdStr = "replace into profitLoss select account,date,monthEnd,{0},mtd_pnl,dtd_pnl,till_ME_pnl from profitLoss where date={1} and account = {2}"
addOptionValueStr = "replace into settlement select p.account, p.date, p.lastDate, p.today, p.equity + sum(long_value)-sum(short_value) as equity, realised, unrealised,commission,margin,deposit,return from option o join settlement p on o.account=p.account and o.date=p.date where o.account in ({0}) and o.date={1} group by o.date,o.account"
fifoPnLStr = "select commodity,contractDate, inOut, sum(contractPrice*contractQty)/sum(contractQty) as avg_price,sum(contractQty) as qty from commodity where contractDate > '{0}-{1}-00' and contractDate not like 'NaT' group by commodity, contractDate, inOut order by commodity,contractDate, inOut"
commodityListStr = "select distinct commodity from commodity where commodity != ''"
def genDropTable(tableNames):
    resultSql = []
    for table in tableNames:
        resultSql.append('''DROP table IF EXISTS {};'''.format(table))

    return resultSql

profitLossDefinition = '''CREATE TABLE profitLoss
           (account           INT      NOT NULL,
           date               INT      NOT NULL,
           monthEnd           INT      NOT NULL,
           ytd_pnl            REAL     NOT NULL,
           mtd_pnl            REAL     NOT NULL,
           dtd_pnl            REAL     NOT NULL,
           till_ME_pnl        REAL     NOT NULL,
           PRIMARY KEY (date, account));'''
futureDefinition = '''CREATE TABLE future
           (instrument        TEXT     NOT NULL,
           date               INT      NOT NULL,
           commodity          TEXT     NOT NULL,
           account            TEXT     NOT NULL,
           long_quantity      REAL     NULL,
           long_notional      REAL     NULL,
           short_quantity     REAL     NULL,
           short_notional     REAL     NULL,
           PRIMARY KEY (instrument, date, account));'''
optionDefinition = '''CREATE TABLE option
           (contract          TEXT     NOT NULL,
           instrument         TEXT     NOT NULL,
           date               INT      NOT NULL,
           commodity          TEXT     NOT NULL,
           callPut            TEXT     NOT NULL,
           account            TEXT     NOT NULL,
           long_quantity      REAL     NOT NULL,
           long_notional      REAL     NULL,
           short_quantity     REAL     NOT NULL,
           short_notional     REAL     NULL,
           long_value         REAL     NULL,
           short_value        REAL     NULL,
           PRIMARY KEY (contract, date, account));'''
greeksDefinition = '''CREATE TABLE greeks
           (instrument        TEXT     NOT NULL,
           date               INT      NOT NULL,
           commodity          TEXT     NOT NULL,
           futureOption       TEXT     NOT NULL,
           deltaCash          REAL     NOT NULL,
           gammaCash          REAL     NOT NULL,
           vega               REAL     NOT NULL,
           theta              REAL     NOT NULL,
           PRIMARY KEY (instrument, date));'''
settlementDefinition = '''CREATE TABLE settlement
           (account           INT      NOT NULL,
           date               INT      NOT NULL,
           lastDate           REAL     NOT NULL,
           today              REAL     NOT NULL,
           equity             REAL     NOT NULL,
           realised           REAL     NOT NULL,
           unrealised         REAL     NOT NULL,
           commission         REAL     NOT NULL,
           margin             REAL     NOT NULL,
           deposit            REAL     NOT NULL,
           return             REAL     NOT NULL,
           PRIMARY KEY (account, date));'''
marketDataDefinition = '''CREATE TABLE marketData
           (instrument        TEXT     NOT NULL,
           date               INT      NOT NULL,
           settlementPrice    REAL     NOT NULL,
           closePrice         REAL     NOT NULL,
           multiplier_f       REAL     NOT NULL,
           volume_f           REAL     NOT NULL,
           position_f         REAL     NOT NULL,
           PRIMARY KEY (instrument, date));'''
commodityDefinition = '''CREATE TABLE commodity
           (department        TEXT     NOT NULL,
           commodity          TEXT     NOT NULL,
           contractDate       TEXT     NULL,
           contractQty        REAL     NULL,
           contractPrice      REAL     NULL,
           settlementPrice    REAL     NULL,
           settlementQty      REAL     NULL,
           boundOrder         REAL     NULL,
           boundReal          REAL     NULL,
           boundDate          TEXT     NULL,
           client             TEXT     NULL,
           contract           TEXT     NULL,
           receiptDate        TEXT     NULL,
           receipt            REAL     NULL,
           margin             REAL     NULL,
           business           TEXT     NULL,
           inOut              TEXT     NULL,
           date               INT      NOT NULL,
           id                 TEXT     NOT NULL,
           PRIMARY KEY (date, id));'''
inventoryDefinition = '''CREATE TABLE inventory
           (commodity         TEXT     NOT NULL,
           inventory          REAL     NULL,
           price              REAL     NULL,
           id                 TEXT     NOT NULL,
           date               INT      NOT NULL,
           PRIMARY KEY (id));'''
Definitions = [inventoryDefinition, commodityDefinition, marketDataDefinition, profitLossDefinition, futureDefinition, optionDefinition, greeksDefinition, settlementDefinition]


