from decipher.beacon import api
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from ..key import api_key, api_server
import openpyxl
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Border, Alignment, Color, fills, Side
import os
from dataclasses import dataclass
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



@dataclass
class SetData:
    pid: str
    file_name: str
    keyid: str = 'record'
    backup: bool = True
    key: str = api_key
    server: str = api_server
    modify_df: pd.DataFrame = pd.DataFrame()
    delete_df: pd.DataFrame = pd.DataFrame()
    modify_list: list = None
    delete_list: list = None

    def __post_init__(self) :
        self.df = pd.read_excel(self.file_name, dtype=str)
        # self.df.fillna(np.nan, inplace=True)
        self.project_path = f'surveys/selfserve/548/{self.pid}'
        
        # api login
        api.login(self.key, self.server)

        # get variables
        datamap = api.get(f'{self.project_path}/datamap', format='json')
        self.variables = [(v['label'], v['qlabel'], v['vgroup']) for v in datamap['variables']]

        print('✅ It\'s ready to setup')
        # variables name check
        self.chk_variables = [(label, qlabel) for label, qlabel, vgroup in self.variables if (qlabel != None) and (label != qlabel) and (not qlabel in label)]
        if self.chk_variables :
            print('❗ You need to check the name of the variable')
            for label, qlabel in self.chk_variables :
                print(f'  🔹 {label} ▶ qlabel = {qlabel}' )
        
    
    def find(self, check_index=0, modi=['수정'], delete=['삭제']) :
        # modifiy data
        self.df.iloc[:, check_index] = self.df.iloc[:, check_index].str.replace(' ', '')
        self.modify_df = self.df[self.df.iloc[:, check_index].str.contains('|'.join(modi))].copy()
        self.modify_df.drop(self.modify_df.columns[check_index], axis=1, inplace=True)

        # delete data
        self.df.iloc[:, check_index] = self.df.iloc[:, check_index].str.replace(' ', '')
        self.delete_df = self.df[self.df.iloc[:, check_index].str.contains('|'.join(delete))].copy()
        self.delete_df.drop(self.delete_df.columns[check_index], axis=1, inplace=True)
        
        print('✅ The modify/delete data found (modfiy_df/delte_df)')

    def setup(self) :
        # modify
        if not self.modify_df.empty :
            modi_dict = self.modify_df.to_dict('index')
            for idx, md in modi_dict.items() :
                for key, value in md.items() :
                    if pd.isna(value) :
                        md[key] = None
                modi_dict[idx] = md
                
            self.modify_list = list(modi_dict.values())
        
        # delete
        if not self.delete_df.empty :
            delete_dict = self.delete_df.to_dict('index')
            self.delete_list = list(delete_dict.values())
        
        if self.modify_df.empty and self.delete_df.empty :
            modi_dict = self.df.to_dict('index')
            for idx, md in modi_dict.items() :
                for key, value in md.items() :
                    if pd.isna(value) :
                        md[key] = None
                modi_dict[idx] = md
            
            self.modify_list = list(modi_dict.values())
            print('❗ Only modify')
        
        print('✅ Setup complete')

    
    def send(self, test=True, delete_mode='disqualify') :
        if self.modify_list :
            try :
                result = api.put(f'{self.project_path}/data/edit', key=self.keyid, data=self.modify_list, test=test)
            except :
                print('❌ Decipher API error')
            
            if test :
                print('❗ It\'s a test mode. If you want to update at the Decipher, enter the \'False\' at the test argument')
            else :
                print('✅ Data update complete')

            stats = result['stats']
            bad = stats['bad']
            if bad :
                print(' ❌ This found a bad samples in file. Please check modifed data.')
            for key, value in stats.items() :
                print(f' 🔹 {key} : {value}')
            print('-'*8)
            print('')
        else :
            print('❗ The modfiy_list is empty')


