# -*- coding: utf-8 -*-
# Create time   : 2023-06-28
# Author        : ZhanYin
# File name     : util.py
# Project name  : OFRisk
import xlrd
import xlwt
import csv
import openpyxl
import re
import os
import datetime


def _dv(dict_var, key, default_value=0):
    return default_value if key not in dict_var else dict_var[key]


class MultiBillCompute:
    def __init__(self):
        self.columns = {}
        self.data = {}
        self.load_config()

    @staticmethod
    def find_key(sheet, key, value_offset_r, value_offset_c, offset_rows=0, offset_cols=0):
        _vr = value_offset_r
        _vc = value_offset_c
        merged = sheet.merged_cells
        for i in range(offset_rows, sheet.nrows):
            for j in range(offset_cols, sheet.ncols):
                cell = sheet.cell_value(i, j)
                if isinstance(cell, str) and key in cell:
                    if _vr ^ _vc == 1:  # 只有数据在标题右侧或下方一格时处理合并单元格(其中一个为1，另一个为0）
                        for mr_low, mr_high, mc_low, mc_high in merged:
                            if mr_low <= i < mr_high and mc_low <= j < mc_high:
                                if _vc == 1:
                                    _vc = mc_high - mc_low
                                elif _vr == 1:
                                    _vr = mr_high - mr_low
                    return i, j, sheet.cell_value(i + _vr, j + _vc)
        return None, None, None

    def load_config(self):
        conf_file = os.path.join(os.path.dirname(__file__), 'config.xlsx')
        wb = xlrd.open_workbook(conf_file)
        for ws in wb.sheets():
            for i in range(ws.ncols):
                txt = ws.cell_value(0, i)
                match1 = re.match(r'\[(\D+)\](\D+)', txt)
                if match1:
                    _dir = match1.group(1)
                    _col = match1.group(2)
                    if _dir not in self.columns:
                        self.columns[_dir] = []
                    self.columns[_dir].append(_col)
                elif '=' in txt:
                    continue
                elif txt not in self.columns:
                    self.columns['main_col'] = txt
                    continue

    def load_file(self, fname):
        dir_name = ''
        for k, v in self.columns.items():
            if k != 'main_col':
                if k in fname:
                    dir_name = k
                    break
        wb = xlrd.open_workbook(fname)
        ws = wb.sheet_by_index(0)
        _row, _col, main_id = self.find_key(ws, self.columns['main_col'], 0, 1)
        if main_id not in self.data:
            self.data[main_id] = {}
        if dir_name not in self.data[main_id]:
            self.data[main_id][dir_name] = {}
        for k in self.columns[dir_name]:
            _row, _col, v = self.find_key(ws, k, 0, 1)
            value = v
            try:
                if type(value) == str:
                    value = float(value.replace(',', ''))
            except ValueError:
                pass
            self.data[main_id][dir_name][k] = value

    def load_data_dir(self):
        dir_name = []
        for k, v in self.columns.items():
            if k != 'main_col':
                dir_name.append(k)
        for root, dirs, files in os.walk(os.path.dirname(os.path.abspath(__file__))):
            for _file in files:
                f = os.path.join(root, _file)
                for d in dir_name:
                    match = re.match(r'.*\\' + d + r'\\.*\.xlsx?$', f)
                    if match:
                        self.load_file(f)
                        print('读取 ' + f)

    def fill_sheet(self):
        lost_data = {}
        for _k, _v1 in self.data.items():
            for _dir, _v2 in _v1.items():
                if _dir not in lost_data:
                    lost_data[_dir] = []
                    for k, v in _v2.items():
                        if v is None:
                            lost_data[_dir].append(k)
        is_lost = False
        for _k, _v in lost_data.items():
            if _v:
                print(_k + "目录下文件未找到 " + ", ".join(_v) + " 字段，请检查！")
                is_lost = True
        if is_lost:
            os.system('pause')
            exit(0)

        today = datetime.datetime.today().strftime('%Y%m%d')
        new_file = today + ".xlsx"
        if os.path.exists(new_file):
            try:
                os.remove(new_file)
            except PermissionError:
                print("另一个程序正在使用此文件，进程无法访问。:" + new_file)
                os.system('pause')
                exit(0)
        wb = openpyxl.load_workbook('config.xlsx')
        sheet_name = wb.sheetnames
        for s in sheet_name:
            ws = wb[s]
            _r = 2
            for k, v in self.data.items():
                for _c in range(1, ws.max_column + 1):
                    cell = ws.cell(1, _c)
                    table_header = cell.value
                    match = re.match(r'\[(\D+)\](\D+)', table_header)
                    if match:
                        _dir = match.group(1)
                        _col = match.group(2)
                        try:
                            ws.cell(_r, _c, self.data[k][_dir][_col])
                        except KeyError:
                            pass
                    elif '=' in table_header:
                        ws.cell(_r, _c, table_header.replace('1', str(_r)))
                    else:
                        ws.cell(_r, _c, k)
                _r += 1
            ws.cell(_r, 1, '合计')
            for _c in range(2, ws.max_column + 1):
                cell = ws.cell(2, _c)
                value = cell.value
                if type(value) in (int, float) or '=' in value:
                    sum_str = '=sum(' + cell.column_letter + '2:' + cell.column_letter + str(_r - 1) + ')'
                    ws.cell(_r, _c, sum_str)
        wb.save(filename=new_file)
        print('生成结果文件： ' + new_file)


if __name__ == '__main__':
    a = MultiBillCompute()
    a.load_data_dir()
    a.fill_sheet()




