from decipher.beacon import api
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from ..key import api_key, api_server
import openpyxl
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Border, Alignment, Color, fills, Side
import os
from decipherAutomatic.getFiles import *

def find_highlight_cells(
        file_name,
        key_variable = 'record',
        start_row_num = 1,
        start_col_num = 1,
        mkdir=False) :
    
    if file_name == '' or file_name == None :
        print('❌ Please check the file_name')
        return
    
    if key_variable == '' or file_name == None :
        print('❌ Please check the key_variable')
        return
    
    if type(start_row_num) != int :
        print('❌ The start_row_num arg is a number only')
        return

    if start_row_num == None :
        print('❌ Please check the start_row_num')
        return

    if type(start_col_num) != int :
        print('❌ The start_col_num arg is a number only')
        return

    if start_col_num == None :
        print('❌ Please check the start_col_num')
        return

    if type(mkdir) != bool :
        print('❌ The mkdir arg is a boolean only')
        return

    # make files
    now = datetime.now()
    year = now.year
    month = '{:02}'.format(now.month)
    day = '{:02}'.format(now.day)
    file_verison = 1

    key_variable = 'record'

    # find high light cell
    wb = load_workbook(file_name, data_only=True)
    st = wb[wb.sheetnames[0]]
    wb_rows = range(1, st.max_row+1)
    wb_cols = range(1, st.max_column+1)

    set_rows = [start_row_num] # 0 is name of variables 
    set_cols = [start_col_num]

    for row in wb_rows :
        for col in wb_cols :
            curr_cell = st.cell(row=row, column=col)
            if not curr_cell.fill.start_color.index == '00000000' :
                set_rows.append(row)
                set_cols.append(col)
            
            if curr_cell.value == key_variable :
                set_cols.append(col)

    set_rows = list(set(set_rows))
    set_cols = list(set(set_cols))

    set_wb = openpyxl.Workbook()
    set_wb.active.title = 'update'
    ws = set_wb.active

    cell_color = PatternFill(start_color="ff9999", fill_type="solid")

    row_cnt = 0
    for row in wb_rows :
        col_cnt = 0
        if row in set_rows : 
            row_cnt += 1
            for col in wb_cols :
                if not col in set_cols :
                    continue
                col_cnt +=1 
                set_cell = ws.cell(row=row_cnt, column=col_cnt)
                chk_cell = st.cell(row=row, column=col)
                set_cell.value = chk_cell.value
                if not chk_cell.fill.start_color.index == '00000000' :
                    set_cell.fill = cell_color
    
    save_file_name = f'find_cells_v{file_verison}.xlsx'
    folder_name = f'{year}{month}{day}'

    if mkdir :
        chk_mkdir(folder_name)

    while True :
        if mkdir :
            save_file_name = f'find_cells_v{file_verison}.xlsx'
            path_join = os.path.join(folder_name, save_file_name)
            if not os.path.exists(path_join) :
                break
            else :
                file_verison += 1
        else :
            save_file_name = f'find_cells_{folder_name}_v{file_verison}.xlsx'
            if not os.path.exists(save_file_name) :
                break
            else :
                file_verison += 1

    save_path = path_join if mkdir else save_file_name
    set_wb.save(save_path)

    os.startfile(save_path)

    print(f'✅ Finding highlight cells is done')
    print(f' 📗 File name is \'{save_file_name}\'')


def get_file_names(
    dir='', 
    excel_only=True,
    recent=False) :

    if not type(dir) == str :
        print('❌ The dir arg is a string only')
        return
    
    if not type(excel_only) == bool :
        print('❌ The excel_only arg is a boolean only')
        return
    
    if not type(recent) == bool :
        print('❌ The recent arg is a boolean only')
        return
    
    file_list = []
    if dir == '' or dir == None :
        file_list = os.listdir()
    else :
        try :
            file_list = os.listdir(dir)
        except :
            print('❌ Path Error')

    if excel_only :
        file_list = [f for f in file_list if 'xlsx' in f or 'xls' in f]

    if not file_list :
        print('❗ This folder is empty')
        return 

    if recent :
        file_recent_chk = []
        for f_name in file_list :
            set_path = os.path.join(dir, f_name) if not dir == '' or dir == None else f_name
            written_time = os.path.getatime(set_path)
            file_recent_chk.append((os.path.join(dir, f_name), written_time))

        sorted_file_list = sorted(file_recent_chk, key=lambda x : x[1], reverse=True)
        return sorted_file_list[0][0]
    else :
        return file_list

