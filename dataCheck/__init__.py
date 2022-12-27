import pandas as pd
import numpy as np
import types
import openpyxl
import re
import nbformat as nbf
from collections import OrderedDict
import os
from zipfile import ZipFile
import json
from decipher.beacon import api
import time
from ..key import api_key, api_server
from decipherAutomatic.getFiles import *

def with_cols_check(with_cols) :
    if type(with_cols) != list :
        print("❌ [ERROR] Type of with_cols must be list")
        return True

def df_err_check(df, err) :
    if type(df) != bool :
        print("❌ [ERROR] The type of df must be bool")
        return True

    if type(err) != bool :
        print("❌ [ERROR] The type of err must be bool")
        return True

def df_err_return(df, df_return, err, err_return) :
        if df and not err :
            return df_return
        
        elif not df and err :
            return err_return
        
        elif df and err :
            return {"df": df_return, "err": err_return}
        
        else :
            return False
    
def sa_check(sa) :
    if not sa or type(sa) != str :
        print("❌ [ERROR] Please check variable name / Type must be str")
        print(" example) 'Q1'")
        return True

def ma_check(ma, cols, len_chk=True) :
    if not ma :
        print("❌ [ERROR] Please check variable names")
        print(" example1) ['Q1r1', 'Q1r2', 'Q1r3']")
        print(" example2) ('Q1r1', 'Q1r3')")
        return True

    if ma and type(ma) != list and type(ma) != tuple :
        print("❌ [ERROR] Type of variable must be list or tuple")
        return True
    
    if len_chk :
        if len(ma) < 2 :
            print("❌ [ERROR] Variable must be 2 length or more")
            return True

        if type(ma) == tuple :
            if len(ma) != 2 :
                print("❌ [ERROR] The variable must be include 2 argument")
                return True
            else :
                first_index = cols.index(ma[0])
                last_index = cols.index(ma[1])

                if first_index > last_index :
                    print(f"❌ [ERROR] Please check the column inedx / current index ( {first_index}-{last_index} )")
                    return True

def ma_return(ma, cols) :
    if type(ma) == tuple :
        first_index = cols.index(ma[0])
        last_index = cols.index(ma[1]) + 1
        ma_cols = cols[first_index:last_index]

    elif type(ma) == list :
        ma_cols = ma

    return ma_cols

def cond_check(cond, add_text=None) :
    if type(cond) == pd.core.series.Series :
        return True
    else :
        if cond == None :
            return None
        else :
            if add_text :
                print(f"❌ [ERROR] {add_text}")
            print("❌ [ERROR] Type of cond must be pandas.core.series.Series type")            
            return False

    
def list_check(_list, add_text="") :
    if _list != None and type(_list) != list :
        print(f"❌ [ERROR] Type of {add_text} must be list")
        return True

def int_check(_int, add_text="") :
    if _int != None and type(_int) != int :
        print(f"❌ [ERROR] Type of {add_text} must be int")
        return True

def str_check(_str, add_text="") :
    if _str != None and type(_str) != str :
        print(f"❌ [ERROR] Type of {add_text} must be str")
        return True
    
def none_check(_none, add_text="") :
    if _none == None :
        print(f"❌ [ERROR] Please check {add_text}")
        return True

def sum_list(*args) :
    return sum([*args], [])

def key_id_check(base, var, var_type) :
    qid = base[0]
    qids = [t for t in qid]
    qids.reverse()
    for q in qids :
        if not q.isdigit() :
            break
        else :
            qid = qid[:-1]
            
    for ma in base :
        if not qid in ma :
            print("❌ [ERROR] Pleas check multi question variable names")
            print(f"{var_type} variable name : {var}")
            print(f"Base MA variable key name : {qid}")
            return {"ok": False, "return": base}

    return {"ok": True, "return": qid}

def not_empty_cell(cell, row) :
    a = cell.cell(row, 1).value
    b = cell.cell(row, 2).value
    c = cell.cell(row, 3).value
    
    if a or b or c :
        return True
    else :
        return False

def re_big(txt) :
    re_chk = re.search(r'\[(.*?)\]',txt)
    if re_chk :
        return re_chk.group(1).strip()
    else :
        return None

def colon_split(txt, num) :
    re_chk = txt.split(":")
    if re_chk :
        return re_chk[num].strip()
    else :
        return None


class Ready :
    def __init__(self, dataframe, key_id='record', include_cols=[]) :
        self.df = dataframe
        self.back_df = self.df.copy()
        self.key_id = key_id
        self.default_show_cols = sum([[self.key_id], include_cols],[])
        self.rows = list(self.df.index)
        self.cols = list(self.df.columns)
        self.separator = "-"*10 + "\n\n"
        self.err_col = 'err'
        self.only_col = 'only_err'
        self.count_label = "count"
        self.exist_col = "exist"
        self.masa_label = "missing_col"
        self.ma_base = "ma_base"
        self.ma_answer = "ma_answer"
    
    # DATA MUTATION
    def change_df(self, new_dataframe) :
        self.df = new_dataframe
        return self.df
    
    def reset(self) :
        self.df = self.back_df.copy()
        return self.df
    
    def apply_col(self, col, apply=None, with_cols=None, cond=None, axis="row") :
        show_cols = self.default_show_cols.copy()
        
        if none_check(col, add_text="column name") : return
        if str_check(col, add_text="column name") : return
        
        if not axis in ["row", "col"] :
            print("❌ [ERROR] axis is available only 'row' or 'col'")
            return 

        if not with_cols == None :
            if ma_check(with_cols, self.cols, len_chk=False) : return
            with_cols = ma_return(with_cols, self.cols)
        else :
            with_cols = []
        
        axis = 1 if axis == "row" else 0
        
        show_cols = sum_list(show_cols, [col], with_cols)
        
        curr_df = self.df.copy()
        cond_flag = cond_check(cond)        
        if cond_flag == False : return
        
        if apply == None :
            curr_df[col] = np.nan
        
        if type(apply) == types.FunctionType :
            try :
                if cond_flag :
                    curr_df[col] = curr_df[cond].apply(apply, axis=axis)
                else :
                    curr_df[col] = curr_df.apply(apply, axis=axis)
            except :
                print("❌ [ERROR] The apply argument insert lambda function")
                print(" example) apply=lambda x: example_function(x.SQ1, x.SQ2)")
                return
        
        else :
            if cond_flag :
                curr_df.loc[cond, col] = apply
            else :
                curr_df[col] = apply
        
        self.df = curr_df.copy()
        
        return self.df[show_cols]
    
    def count_col(self, col, variables, value=None, with_cols=None, cond=None) :
        show_cols = self.default_show_cols.copy()

        if none_check(col, add_text="column name") : return
        if str_check(col, add_text="column name") : return
        
        if ma_check(variables, self.cols) : return
        ma_cols = ma_return(variables, self.cols)
        
        if not with_cols == None :
            if ma_check(with_cols, self.cols, len_chk=False) : return
            with_cols = ma_return(with_cols, self.cols)
        else :
            with_cols = []
        
        if value != None :
            if not type(value) in [list, int] :
                print("❌ [ERROR] The value must be list or int")
                return

        cond_flag = cond_check(cond)        
        if cond_flag == False : return
        
        show_cols = sum_list(show_cols, [col], ma_cols, with_cols)
        
        curr_df = self.df.copy()
        curr_df[col] = np.nan
        if cond_flag :
            cond_idx = list(curr_df[cond].index)
        else :
            cond_idx = list(curr_df.index)
            
        for idx in cond_idx :
            answers = list(curr_df.loc[idx, ma_cols])
            
            answer_cnt = 0
            if type(value) == int :
                answer_cnt = answers.count(value)
                
            elif type(value) == list :
                for v in value :
                    if v in answers :
                        answer_cnt += 1
            else :
                for a in answers :
                    if not pd.isnull(a) and not a == 0 :
                        answer_cnt += 1

            curr_df.loc[idx, col] = answer_cnt
        
        self.df = curr_df.copy()
        
        return self.df[show_cols]

    def sum_col(self, col, variables, with_cols=None, cond=None) :
        show_cols = self.default_show_cols.copy()

        if none_check(col, add_text="column name") : return
        if str_check(col, add_text="column name") : return
        
        if ma_check(variables, self.cols) : return
        ma_cols = ma_return(variables, self.cols)
        
        if not with_cols == None :
            if ma_check(with_cols, self.cols, len_chk=False) : return
            with_cols = ma_return(with_cols, self.cols)
        else :
            with_cols = []
        
        cond_flag = cond_check(cond)        
        if cond_flag == False : return
        
        show_cols = sum_list(show_cols, [col], ma_cols, with_cols)
        
        curr_df = self.df.copy()
        curr_df[col] = np.nan
        if cond_flag :
            curr_df.loc[cond, col] = curr_df[ma_cols].sum(axis=1)
        else :
            curr_df.loc[:, col] = curr_df[ma_cols].sum(axis=1)
        
        self.df = curr_df.copy()
        
        return self.df[show_cols]
    
    # DATA TABLE SHOW
    def freq(self, qids) :
        if type(qids) == str :
            print(self.df[qids].value_counts())
        elif type(qids) in [list, tuple] :
            ma_cols = ma_return(qids, self.cols)
            freq_list = [(qid, self.df[qid].value_counts()) for qid in ma_cols ]
            for qid, fq in freq_list:
                print(f"💠 {qid}")
                print(fq)
                print(self.separator)
                print("")
        else :
            print("❌ [ERROR] Type of qid must be str or list or tuple")
    
    def crosstabs(self, *qids) :
        for qid in qids :
            if type(qid) != str :
                print("❌ [ERROR] Type of qid must be str and max 3")
        
        if not len(qids) in range(2, 4) :
            print("❌ [ERROR] The qids is atleast 2 and atmost 3")
            
        if len(qids) == 2 :
            return pd.crosstab(self.df[qids[0]], self.df[qids[1]], margins=True)
        
        if len(qids) == 3 :
            return pd.crosstab([self.df[qids[0]], self.df[qids[1]]], self.df[qids[2]], margins=True)
    
    # DATA CHECK FUNCTION
    def safreq(self, sa=None, cond=None, with_cols=None, only=None, df=False, err=False) :
        show_cols = self.default_show_cols.copy()
        if df_err_check(df, err) : return
        
        if sa_check(sa) : return

        if not with_cols == None :
            if ma_check(with_cols, self.cols, len_chk=False) : return
            with_cols = ma_return(with_cols, self.cols)
        else :
            with_cols = []
        
        curr_df = self.df.copy()
        err_col = self.err_col
        only_col = self.only_col
        curr_df[err_col] = np.nan
        curr_df[only_col] = np.nan
        
        sa_cols = [err_col, only_col, sa]
        show_cols = sum_list(show_cols, sa_cols, with_cols)
        
        cond_flag = cond_check(cond)        
        if cond_flag == False : return

        only_text = ""
        if not only == None :
            if type(only) == range :
                only = list(only)
                only.append(only[-1]+1)
                only_filt = curr_df[sa].isin(only)
                only_text = f"Range {only[0]} THRU {only[-1]}"
                
            elif type(only) == list :
                only_filt = curr_df[sa].isin(only)

                if len(only) > 6 :
                    only_text = f"List [{only[0]}, {only[1]}, ... , {only[-2]}, {only[-1]}]"
                else :
                    only_text = f"List {only}"
                
            elif type(only) in [str, int] :
                only_filt = curr_df[sa]==only
                only_text = only
            else :
                print("❌ [ERROR] Type of only must be range or list or str or int")
                return
        
        print_str = ""
        print_str += f"📢 {sa} MISSING CHECK\n"

        if not cond_flag == True : 
            print_str += f"  💠 All base\n"
            ms_chk = list(curr_df[curr_df[sa].isnull()].index)
            if ms_chk :
                curr_df.loc[ms_chk, 'err'] = 'missing'
        else :
            print_str += f"  💠 Condition\n"
            ms_chk = list(curr_df[(curr_df[sa].isnull()) & (cond)].index)
            if ms_chk :
                curr_df.loc[ms_chk, err_col] = 'missing'
            
            over_chk = list(curr_df[(~curr_df[sa].isnull()) & ~(cond)].index)
            if over_chk :
                curr_df.loc[over_chk, err_col] = 'base'

            resp_chk = list(curr_df[cond].index)
            if not resp_chk :
                print_str += "  ❓ No response to this condition\n"

        

        err_chk = list(curr_df[~curr_df[err_col].isnull()].index)
        
        if not err_chk :
            print_str += f"  ✅ No error\n"
        else :
            print_str += f"  ❌ Error sample count : {len(err_chk)}\n"

        print_str += self.separator

        if not only == None :
            print_str += f"📢 {sa} ANSWER DATA CHECK\n"
            print_str += f"  💠 Answer only in {only_text}\n"
            
            only_chk = list(curr_df[~only_filt].index)
            if cond_flag == True : 
                only_chk = list(curr_df[(~only_filt) & (cond)].index)
            
            if not only_chk :
                print_str += f"  ✅ Only value check : No error\n"
            else :
                curr_df.loc[only_chk, only_col] = 'chk'
                print_str += f"  ❌ Only Error sample count : {len(only_chk)}\n"
        
            print_str += self.separator

        err_df = curr_df[ (~curr_df[err_col].isnull()) | (~curr_df[only_col].isnull()) ][show_cols].copy()

        curr_df[err_col] = curr_df[err_col].fillna('')
        curr_df[only_col] = curr_df[only_col].fillna('')
        err_df[err_col] = err_df[err_col].fillna('')
        err_df[only_col] = err_df[only_col].fillna('')
        
        return_df = curr_df[cond][show_cols] if cond_flag == True else curr_df[show_cols]
        
        outputs = df_err_return(df, return_df, err, err_df)
        if type(outputs) == bool and outputs == False :
            print(print_str)
        else :
            return outputs

    def mafreq(self, ma, cond=None, with_cols=None, atleast=1, atmost=None, exactly=None, df=False, err=False) :
        show_cols = self.default_show_cols.copy()
        if df_err_check(df, err) : return
        
        if ma_check(ma, self.cols) : return
        ma_cols = ma_return(ma, self.cols)

        if not with_cols == None :
            if ma_check(with_cols, self.cols, len_chk=False) : return
            with_cols = ma_return(with_cols, self.cols)
        else :
            with_cols = []
        
        cnt_col = self.count_label
        
        curr_df = self.df.copy()
        
        err_col = self.err_col
        curr_df[err_col] = np.nan
        
        cond_flag = cond_check(cond)        
        if cond_flag == False : return

        err_col = self.err_col
        show_cols = sum_list(show_cols, [cnt_col, err_col], ma_cols, with_cols)
        
        for idx in list(curr_df.index) :
            cnt = 0 
            values = list(curr_df.loc[idx, ma_cols])
            for v in values :
                if not pd.isnull(v) and v != 0 :
                    cnt += 1
            curr_df.loc[idx, cnt_col] = cnt
        
        count_list = [
            {
                "type" : "atleast",
                "value" : atleast,
                "cond" : curr_df[cnt_col] < atleast
            },
            {
                "type" : "atmost",
                "value" : atmost,
                "cond" : curr_df[cnt_col] > atmost
            },
            {
                "type" : "exactly",
                "value" : exactly,
                "cond" : curr_df[cnt_col] != exactly
            }
         ]
        
        for ck in count_list :
            if int_check(ck["value"], add_text=ck["type"]) : return
                
        start = ma[0]
        end = ma[-1]
        
        print_str = ""
        print_str += f"📢 '{start} - {end}' Answer Check\n"
        
        if not cond_flag == True : 
            print_str += f"  💠 All base\n"
        else :
            print_str += f"  💠 Condition\n"
            
        for item in count_list :
            check_value = item["value"]
            if not check_value == None :
                check_type = item["type"]
                if not cond_flag == True : 
                    err_chk = list(curr_df[item["cond"]].index)
                    if err_chk :
                        curr_df.loc[err_chk, err_col] = check_type
                else :
                    err_chk = list(curr_df[(item["cond"]) & (cond)].index)
                    if err_chk :
                        curr_df.loc[err_chk, err_col] = check_type
                        
                print_str += f"  💠 The {check_type} error check ({check_type} = {check_value})\n"
        
        if cond_flag == True : 
            err_chk = list(curr_df[(curr_df[cnt_col]>0) & ~(cond)].index)
            if err_chk :
                curr_df.loc[err_chk, err_col] = 'base'

            resp_chk = list(curr_df[cond].index)
            if not resp_chk :
                print_str += "  ❓ No response to this condition\n"
        

        err_chk = list(curr_df[~curr_df[err_col].isnull()].index)
        if err_chk :
            print_str += f"  ❌ Error sample count : {len(err_chk)}\n"
        else :
            print_str += f"  ✅ No error\n"
        print_str += self.separator

        err_df = curr_df[~curr_df[err_col].isnull()][show_cols].copy()
        curr_df[err_col] = curr_df[err_col].fillna('')
        err_df[err_col] = err_df[err_col].fillna('')
        
        return_df = curr_df[cond][show_cols] if cond_flag == True else curr_df[show_cols]
        
        outputs = df_err_return(df, return_df, err, err_df)
        if type(outputs) == bool and outputs == False :
            print(print_str)
        else :
            return outputs

    def dupchk(self, ma, with_cols=None, okUnique=None, df=False, err=False) :
        show_cols = self.default_show_cols.copy()
        if df_err_check(df, err) : return
        
        if ma_check(ma, self.cols) : return
        rk_cols = ma_return(ma, self.cols)

        if not with_cols == None :
            if ma_check(with_cols, self.cols, len_chk=False) : return
            with_cols = ma_return(with_cols, self.cols)
        else :
            with_cols = []
            
        if not okUnique == None :
            if not type(okUnique) in [list, range, int, str] :
                print("❌ [ERROR] Type of okUnique must be list or int or str")
                return
            
            if type(okUnique) == range :
                okUnique = list(okUnique)
                okUnique.append(okUnique[-1]+1)
            elif type(okUnique) in [int, str] :
                okUnique = [okUnique]
        else :
            okUnique = []

        dup_df = self.df.copy()
        raw_index = list(dup_df.index)
        
        dup_col = 'dupchk'
        dup_df[dup_col] = np.nan
        
        show_cols = sum_list(show_cols, [dup_col], rk_cols, with_cols)
        
        for idx in raw_index :
            r = dup_df.loc[idx ,rk_cols]
            answers = list(r)
            dup_del = set(answers)

            dup_values = []
            for dup in dup_del :
                if not pd.isnull(dup) and not dup in okUnique:
                    cnt = answers.count(dup)
                    if cnt > 1 :
                        dup_values.append(dup)
            
            dup_df.loc[idx, [dup_col]] = str(dup_values) if dup_values else np.nan
        
        check_row = dup_df[~dup_df[dup_col].isnull()]
        check_row_index = list(check_row.index)
        
        print_str = ""
        
        start = rk_cols[0]
        end = rk_cols[-1]
        
        print_str += f"📢 '{start} - {end}' Duplicated value check\n"
        print_str += f"  💠 okUnique = {okUnique}\n"
        if not check_row_index :
            print_str += f"  ✅ Answer is not duplicated\n"
        else :
            print_str += f"  ❌ Error sample count : {len(check_row_index)}\n"

        print_str += self.separator
        
        dup_df[dup_col] = dup_df[dup_col].fillna('')
        
        outputs = df_err_return(df, dup_df[show_cols], err, check_row[show_cols])
        if type(outputs) == bool and outputs == False :
            print(print_str)
        else :
            return outputs


    def logchk(self, input_cond, output_cond, with_cols=None, df=False, err=False):
        show_cols = self.default_show_cols.copy()
        if df_err_check(df, err) : return

        error_flag = False
        
        input_flag = cond_check(input_cond, 'input_cond') 
        output_flag = cond_check(output_cond, 'output_cond')
        for flag in [input_flag, output_flag] :
            if flag == False : return
            
        if not with_cols == None :
            if ma_check(with_cols, self.cols, len_chk=False) : return
            with_cols = ma_return(with_cols, self.cols)
        else :
            with_cols = []
            
        curr_df = self.df.copy()
        err_col = self.err_col
        curr_df[err_col] = np.nan
    
        check_index = list(curr_df[(input_cond) & ~(output_cond)].index)
        
        show_cols = sum_list(show_cols, [err_col], with_cols)
        print_str = ""
        print_str += "📢 Punching Logic Check\n"

        resp_chk = list(curr_df[input_cond].index)
        if not resp_chk :
            print_str += "  ❓ No response to this condition\n"
        else :
            if len(check_index) == 0 :
                print_str += f"  ✅ Punching Logic correct\n"
            else :
                curr_df.loc[check_index, err_col] = 'chk'
                print_str += f"  ❌ [ERROR] Punching Logic Error\n"
                print_str += f"  ❌ Error sample count : {len(check_index)}\n"
        
        print_str += self.separator
        err_df = curr_df[~curr_df[err_col].isnull()].copy()
        err_df[err_col] = err_df[err_col].fillna('')
        curr_df[err_col] = curr_df[err_col].fillna('')
        
        outputs = df_err_return(df, curr_df[input_cond][show_cols], err, err_df[show_cols])
        if type(outputs) == bool and outputs == False :
            print(print_str)
        else :
            return outputs

    def masa(self, ma, sa, with_cols=None, df=False, err=False):
        show_cols = self.default_show_cols.copy()
        if df_err_check(df, err) : return

        if ma_check(ma, self.cols) : return
        ma_cols = ma_return(ma, self.cols)
        
        key_id = key_id_check(ma_cols, sa, "SA")
        
        ma_qid = ""
        if key_id["ok"] :
            ma_qid = key_id["return"]
        else :
            return key_id["return"]

        if sa_check(sa) : return

        if not with_cols == None :
            if ma_check(with_cols, self.cols, len_chk=False) : return
            with_cols = ma_return(with_cols, self.cols)
        else :
            with_cols = []
            
        masa_cols = [sa] + ma_cols
        
        ms_col = self.masa_label
        exist = self.exist_col
        ma_base = self.ma_base
        
        show_cols = sum_list(show_cols, [ms_col, exist, ma_base], masa_cols, with_cols)    
        curr_df = self.df.copy()
        curr_df[ms_col] = np.nan
        curr_df[exist] = np.nan
        curr_df[ma_base] = np.nan
    
        filt_df = curr_df[~curr_df[sa].isnull()].copy()
        filt_index = list(filt_df.index)
        
        print_str = ""
        print_str += "📢 Multi variable base Single variable Logic Check\n"
        print_str += f"  💠 SA : {sa}\n"
        print_str += f"  💠 MA : {ma_cols[0]} - {ma_cols[-1]} ({len(ma_cols)} columns)\n"
        
        for idx in filt_index :
            v = int(filt_df.loc[idx, sa])
            base = f"{ma_qid}{v}"
            curr_base = [col.replace(ma_qid, '') for col in ma_cols if not pd.isnull(filt_df.loc[idx, col]) and filt_df.loc[idx, col] != 0]
            filt_df.loc[idx, ma_base] = str(curr_base)
            if base in ma_cols :
                base_v = filt_df.loc[idx, base]
                if pd.isnull(base_v) or base_v.astype(int) == 0 :
                    filt_df.loc[idx, ms_col] = base
            else :
                filt_df.loc[idx, exist] = base
        
        err_index = list(filt_df[~filt_df[ms_col].isnull()].index)
        exist_check = list(filt_df[~filt_df[exist].isnull()].index)
        
        err_df = filt_df[~(filt_df[ms_col].isnull()) | ~(filt_df[exist].isnull())][show_cols]
        if err_index or exist_check:
            if err_index :
                print_str += f"  ❌ [ERROR] MA-SA Logic Error\n"
                print_str += f"  ❌ Error sample count : {len(err_index)}\n"
            if exist_check :
                print_str += f"   ❗ [WARNING] Exist Variable Error\n"
                print_str += f"  ❌ Error sample count : {len(exist_check)}\n"
        else :
            print_str += f"  ✅ Logic correct\n"

        print_str += self.separator
        
        
        filt_df[ms_col] = filt_df[ms_col].fillna('')
        filt_df[exist] = filt_df[exist].fillna('')
        err_df[ms_col] = err_df[ms_col].fillna('')
        err_df[exist] = err_df[exist].fillna('')
        
        outputs = df_err_return(df, filt_df[show_cols], err, err_df)
        if type(outputs) == bool and outputs == False :
            print(print_str)
        else :
            return outputs

    def mama(self, base_ma, ma, with_cols=None, df=False, err=False):
        show_cols = self.default_show_cols.copy()
        if df_err_check(df, err) : return
        
        if ma_check(base_ma, self.cols) : return
        base_ma_cols = ma_return(base_ma, self.cols)
        
        if ma_check(ma, self.cols) : return
        ma_cols = ma_return(ma, self.cols)

        if not with_cols == None :
            if ma_check(with_cols, self.cols, len_chk=False) : return
            with_cols = ma_return(with_cols, self.cols)
        else :
            with_cols = []
        
        base_key_id = key_id_check(base_ma_cols, ma_cols, "MA")
        base_ma_qid = ""
        if base_key_id["ok"] :
            base_ma_qid = base_key_id["return"]
        else :
            return base_key_id["return"]

        key_id = key_id_check(ma_cols, ma_cols, "MA")
        ma_qid = ""
        if key_id["ok"] :
            ma_qid = key_id["return"]
        else :
            return key_id["return"]
        
        cols_order = []
        for ma in ma_cols :
            cols_order.append(ma)
            
            swith_qid = ma.replace(ma_qid, base_ma_qid)
            if swith_qid in base_ma_cols :
                cols_order.append(swith_qid)
        
        ms_col = self.masa_label
        exist = self.exist_col
        ma_base = self.ma_base
        ma_answer = self.ma_answer
        
        show_cols = sum_list(show_cols, [ms_col, exist, ma_base, ma_answer], cols_order, with_cols)
        
        curr_df = self.df.copy()
        curr_df[ms_col] = np.nan
        curr_df[exist] = np.nan
        curr_df[ma_base] = np.nan
        curr_df[ma_answer] = np.nan

        filt_df = curr_df[~(curr_df[ma_cols].isnull()).all(axis=1)].copy()
        filt_index = list(filt_df.index)
        
        print_str = ""
        print_str += "📢 Multi variable base Multi variable Logic Check\n"
        print_str += f"  💠 MA : {ma_cols[0]} - {ma_cols[-1]} ({len(ma_cols)} columns)\n"
        print_str += f"  💠 MA : {base_ma_cols[0]} - {base_ma_cols[-1]} ({len(base_ma_cols)} columns)\n"
        
        err_index = []
        for idx in filt_index :
            curr_base = [col.replace(base_ma_qid, '') for col in base_ma_cols if not pd.isnull(filt_df.loc[idx, col]) and filt_df.loc[idx, col] != 0]
            answers = [col.replace(ma_qid, '') for col in ma_cols if not pd.isnull(filt_df.loc[idx, col]) and filt_df.loc[idx, col] != 0]
            filt_df.loc[idx, ma_base] = str(curr_base)
            filt_df.loc[idx, ma_answer] = str(answers)
            
            err_vars = []
            exist_vars = []
            for answer in answers :
                if not pd.isnull(answer) :
                    v = int(answer)
                    base_id = base_key_id["return"]
                    base = f"{base_id}{v}"                    
                    if base in base_ma_cols :
                        base_v = filt_df.loc[idx, base]
                        if pd.isnull(base_v) or base_v.astype(int) == 0 :
                            err_vars.append(base)
                    else :
                        exist_vars.append(base)
                    
                    if err_vars :
                        filt_df.loc[idx, ms_col] = str(err_vars)
                        
                    if exist_vars :
                        filt_df.loc[idx, exist] = str(exist_vars)
                        
        err_index = list(list(filt_df[~filt_df[ms_col].isnull()].index))
        exist_check = list(filt_df[~filt_df[exist].isnull()].index)
        
        err_df = filt_df[~(filt_df[ms_col].isnull()) | ~(filt_df[exist].isnull())][show_cols]

        if err_index or exist_check:
            if err_index :
                print_str += f"  ❌ [ERROR] MA-MA Logic Error\n"
                print_str += f"  ❌ Error sample count : {len(err_index)}\n"
            if exist_check :
                print_str += f"   ❗ [WARNING] Exist Variable Error\n"
                print_str += f"  ❌ Error sample count : {len(exist_check)}\n"
        else :
            print_str += f"  ✅ Logic correct\n"

        print_str += self.separator
        
        filt_df[ms_col] = filt_df[ms_col].fillna('')
        filt_df[exist] = filt_df[exist].fillna('')
        err_df[ms_col] = err_df[ms_col].fillna('')
        err_df[exist] = err_df[exist].fillna('')
        
        outputs = df_err_return(df, filt_df[show_cols], err, err_df)
        if type(outputs) == bool and outputs == False :
            print(print_str)
        else :
            return outputs

def Setting(pid, mode='auto', 
            key=api_key, 
            server=api_server, 
            json_export=True, 
            data_layout=True, 
            datamap_name='Datamap',
            mkdir=False) :
    if pid == '' or not pid :
        print('❌ Please enter pid')
        return

    if not mode in ['auto', 'file'] :
        print('❌ Please check the mode argument (auto or file)')
        return

    parent_path = os.getcwd()
    if mkdir :
        parent_path =  os.path.join(parent_path, pid)
        chk_mkdir(parent_path)

    if mode == 'file' :
        file_name = f'{pid}.xlsx'
        xl = openpyxl.load_workbook(file_name)
        map_sheet = datamap_name
        data_map = xl[map_sheet]
        print('📢 Read excel file (xlsx)')

    if mode == 'auto' :
        file_name = f'{pid}.csv'
        try :
            api.login(key, server)
        except :
            print('❌ Error : Decipher api login failed')
            return

        path = f'surveys/selfserve/548/{pid}'
        # get csv data
        csv_data = api.get(f'{path}/data', format='csv', cond='qualified')

        csv_binary = f'binary_{pid}.csv'
        create_binary_file(parent_path, csv_binary, csv_data)
        create_ascii_file(parent_path, csv_binary, f'{pid}.csv')
        
        time.sleep(3)

        # get datamap xlsx
        map_xlsx = api.get(f'{path}/datamap', format='xlsx')
        create_binary_file(parent_path, f'mapsheet_{pid}.xlsx', map_xlsx)

        xl = openpyxl.load_workbook(os.path.join(parent_path, f'mapsheet_{pid}.xlsx'))
        map_sheet = 'datamap'
        data_map = xl[map_sheet]

        print('📢 Using Decipher REST API (csv)')

    mx_row = data_map.max_row
    mx_col = data_map.max_column

    key_ids = ['record', 'uuid', 'list', 'UID', 'eid', 'GID', 'uid', 'pid']
    diff_vars = ['Agree', 'Chk', 'noanswer', 'date', 'markers', 'status', 'vlist', 'qtime', 'vos', 'vosr15oe', 'vbrowser', 'vbrowserr15oe', 'vmobiledevice', 'vmobileos', 'start_date', 'vdropout', 'source', 'decLang', 'userAgent', 'dcua', 'url', 'session', 'ipAddress', 'qtime', 'HQTolunaEnc']
    all_diff = key_ids + diff_vars
    rank_chk = ['1순위', '2순위', '1st', '2nd']

    na = 'noanswer'
    eltxt = 'element'
    col_name = ["a", "b", "c"]
    curr_var = {col:[] for col in col_name }

    variables = []
    
    #print("  ❌ DataCheck Setting Start")
    for row in range(1, mx_row+1) :
        if not_empty_cell(data_map, row) :
            for idx, col in enumerate(range(1, mx_col+1)) :
                curr_col = col_name[idx]
                curr_dict = curr_var[curr_col]
                cell = data_map.cell(row, col)
                if cell.value or cell.value == 0: 
                    curr_dict.append(cell.value)
                    curr_var[curr_col] = curr_dict
                if cell.value == None or cell.value == "" :
                    curr_dict.append("")
                    curr_var[curr_col] = curr_dict
        else :
            variables.append(curr_var)
            curr_var = {col:[] for col in col_name }


    qids = OrderedDict()
    for variable in variables :
        # qid, type summary
        a_cell = variable['a']
        a_cell = [a for a in a_cell if a != '' and a != None]
        b_cell = variable['b']
        #b_cell = [b for b in b_cell if b != '' and b != None]
        c_cell = variable['c']
        #c_cell = [c for c in c_cell if c != '' and c != None]
        qid = a_cell[0] # qid

        # print(qid)
        # print(b_cell)
        # print(c_cell)

        type_chk = a_cell[1] if len(a_cell) >= 2 else None # type check

        # attribute
        main_qlabel = None
        qtype = None
        qelements = []
        qvalue = None
        qtitle = None

        # labels setting
        qlabes = {
            'values' : {b:c_cell[idx] for idx, b in enumerate(b_cell) if type(b) == int}  if c_cell else {},
            'texts' : {re_big(b):c_cell[idx] for idx, b in enumerate(b_cell) if type(b) == str or b == None} if c_cell else {}
        }
        #  find name in []

        # main q label check
        find_qname = re_big(qid.split(':')[0])
        if find_qname :
            main_qlabel = find_qname
            qelements.append(main_qlabel)
            qtitle = colon_split(qid, 1)
        else :
            main_qlabel = colon_split(qid, 0)
            qtitle = colon_split(qid, 1)

        # type check
        if type_chk :
            open_text = 'OPEN'
            if open_text.upper() in type_chk.upper() :
                qtype = 'OE'
            else :
                qtype = 'CE'

            # value check
            value_text = 'VALUES'
            if value_text.upper() in type_chk.upper() :
                qvalue = colon_split(type_chk, 1)
            else :
                qvalue = None


        else :
            qtype = 'OTHER'

        # other oe check
        oe_chk = 'oe'
        if oe_chk in main_qlabel :
            qtype = 'OTHER_OE'

        # elements setting
        for b in b_cell :
            b_chk = re_big(str(b))
            if b_chk :
                qelements.append(b_chk)

        if not 'OE' in qtype:
            # ma check
            unchk = 'Unchecked'
            c_chk = [c.upper() for c in c_cell if type(c) == str]
            if unchk.upper() in c_chk : 
                qtype = 'MA'
            else :
                # radio/number check
                int_chk = [b for b in b_cell if type(b) == int]
                if int_chk :
                    qtype = 'SA'
                else :
                    qtype = 'NUM'

        if len(qelements) >= 2 :
            if main_qlabel in qelements :
                qelements.remove(main_qlabel)

        el_labels = {key:value for key, value in qlabes['texts'].items() if key}
        
        qids[main_qlabel] = {
            'element' : qelements,
            'title' : qtitle,
            'type' : qtype,
            'value' : qvalue,
            'value_label' : qlabes['values'],
            'element_label' : el_labels,
        }

    if na in list(qids.keys()) :
        nas = qids[na]
        els = nas[eltxt]
        for el in els :
            na_el = el.split('_')[0].replace(na, '')
            if qids[na_el] :
                qel = qids[na_el][eltxt]
                qel.append(el)
                qids[na_el][eltxt] = qel

    # print(qids)

    # default setting
    nb = nbf.v4.new_notebook()
    ipynb_file_name = f'DataCheck_{pid}.ipynb'
    order_qid = list(qids.items())

    # json export
    if json_export :
        with open(os.path.join(parent_path, f'map_{pid}.json'), 'w', encoding='utf-8') as f :
            json.dump(qids, f, ensure_ascii=False, indent=4)

    # data layout export
    if data_layout :
        with open(os.path.join(parent_path, f'layout_{pid}.txt'), 'w', encoding='utf-8') as f :
            # key id setting
            variable_names = [attrs[0] for attrs in order_qid if attrs[0] in key_ids]

            for key in key_ids :
                if not key in variable_names :
                    continue

                if key == 'record' :
                    f.write(f'{key},{key},7\n')
                elif key == 'uuid' :
                    f.write(f'{key},{key},16\n')
                else :
                    f.write(f'{key},{key},60\n')

            # variable setting
            for attrs in order_qid :
                qid = attrs[0]
                els = attrs[1]
                if qid in all_diff :
                    continue

                qels = els['element']
                qtype = els['type']
                qval = els['value']
                qtitle = els['title']
                val_label = els['value_label']
                el_label = els['element_label']

                if qtype in ['OTHER_OE'] :
                    continue
                
                if qtype == 'SA' :
                    if len(val_label) == 1 :
                        # dummy variable
                        continue

                for e in qels :
                    if qtype == 'OE' :
                        if na in e :
                            f.write(f'{e},{e},1\n')
                        else :
                            continue
                    else :
                        max_width = len(qval.split('-')[1])
                        if na in e :
                            max_width = 1
                        f.write(f'{e},{e},{max_width}\n')
                

    # variable py file create
    variable_py_name = f'variables_{pid}.py'
    py_file = open(os.path.join(parent_path, variable_py_name), 'w')
    py_file.write(f'# {pid} variables\n')

    ipynb_cell = []


    # set_file_name = 'pd.read_excel(file_name)' if mode == 'file' else 'pd.read_csv(file_name, low_memory=False)'

    default = f'''from decipherAutomatic.dataCheck import *
import pandas as pd
from variables_{pid} import * 

file_name = '{pid}.xlsx'

raw = pd.read_excel(file_name, engine='openpyxl')

# if data sheet is more than 1
# example)
# df_all = pd.read_excel('XXX.xlsx', sheet_name = None)
# del df_all['MAP_SHEET']
# df_all.keys()
# raw = pd.merge(df_all['A1'], df_all['A2'],  left_index=True, right_index=True, how='left')

dc = Ready(raw)

# show all cols, rows setting
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
df = dc.df
'''

    ipynb_cell.append(nbf.v4.new_code_cell(default))

    # qids setting
    for idx, attrs in enumerate(order_qid) :
        qid = attrs[0]
        els = attrs[1]
        if not qid in all_diff :
            qels = els['element']
            qtype = els['type']
            qval = els['value']
            qtitle = els['title']
            val_label = els['value_label']
            el_label = els['element_label']

            cell_texts = []
            cell_texts.append(f'# {qid} : {qtype}')
            # sa check #
            if qtype == 'SA' or (qtype == 'MA' and len(qels) == 1):
                if qtype == 'SA' :
                    if qval :
                        val_chk = f"# value : {qval}"
                        cell_texts.append(val_chk)

                    if len(qels) >=2 :
                        diff_na = [q for q in qels if not na in q]
                        py_file.write(f"{qid} = {diff_na}\n")

                    for qel in qels :
                        if na in qel :
                            cell_texts.append(f'# The {qid} contains {qel}')
                        else :
                            safreq = f"dc.safreq('{qel}')"
                            py_file.write(f"{qel} = '{qel}'\n")
                            cell_texts.append(safreq)

                    if val_label :
                        values = [v for v in val_label.keys() if not int(v) == 0]
                        py_file.write(f'{qid}_value = {values}\n')
                        #values_txt = f'{qid}_values = {values}'
                        #cell_texts.append(values_txt)

                    # rank check
                    if len(qels) >= 2 :
                        labels = list(el_label.values())
                        rk = []
                        for rk_txt in rank_chk :
                            for label in labels :
                                mu_rk = rk_txt.strip().replace(' ','').upper()
                                mu_label = label.strip().replace(' ','').upper()
                                if mu_rk in mu_label :
                                    rk.append(label)
                        if len(rk) >= 2 :
                            dup_diff_na = [q for q in qels if not na in q]
                            set_qid = f"('{dup_diff_na[0]}', '{dup_diff_na[-1]}')"

                            py_file.write(f"{qid} = {dup_diff_na}\n")
                            #cell_texts.append(f'{qid} = {set_qid}')
                            dupchk = f"dc.dupchk({set_qid})"
                            cell_texts.append(dupchk)
                else :
                    if qval :
                        val_chk = f"# value : {qval}"
                        cell_texts.append(val_chk)
                        safreq = f"dc.safreq('{qels[0]}')"
                        cell_texts.append(safreq)
            ### sa end ###

            # ma check #
            elif qtype == 'MA' :
                diff_na = [q for q in qels if not na in q]
                nas = [q for q in qels if na in q]
                first_el = diff_na[0]
                last_el = diff_na[-1]
                set_qid = f"('{first_el}', '{last_el}')"

                for q in diff_na :
                    py_file.write(f"{q} = '{q}'\n")

                py_file.write(f"{qid} = {diff_na}\n")

                if val_label :
                    values = [v for v in val_label.keys() if not int(v) == 0]
                    if not values == [1] :
                        py_file.write(f'{qid}_value = {values}\n')
                        #values_txt = f'{qid}_values = {values}'
                        #cell_texts.append(values_txt)
                    else :
                        py_file.write(f'{qid}_value = [0, 1]\n')
                # cell_texts.append(f'{qid} = {set_qid}')

                mafreq = f"dc.mafreq({set_qid})"

                cell_texts.append(mafreq)

                if nas :
                    cell_texts.append(f'# The {qid} contains {nas}')
            ### ma end ###


            # num check #
            elif qtype == 'NUM' :
                range_set = None

                if len(qels) >=2 :
                    diff_na = [q for q in qels if not na in q]
                    py_file.write(f"{qid} = {diff_na}\n")

                if qval :
                    values = qval.split('-')
                    range_set = f"only=range({values[0]}, {values[1]})"

                for qel in qels :
                    if na in qel :
                        cell_texts.append(f'# The {qid} contains {qel}')
                    else :
                        if range_set :
                            safreq = f"dc.safreq('{qel}', {range_set})"
                        else :
                            safreq = f"dc.safreq('{qel}')"
                        py_file.write(f"{qel} = '{qel}'\n")
                        cell_texts.append(safreq)

            ### num end ###

            # text check #
            elif qtype == 'OE' :
                if len(qels) >=2 :
                    diff_na = [q for q in qels if not na in q]
                    py_file.write(f"{qid} = {diff_na}\n")

                for qel in qels :
                    if na in qel :
                        cell_texts.append(f'# The {qid} contains {qel}')
                    else :
                        safreq = f"dc.safreq('{qel}')"
                        py_file.write(f"{qel} = '{qel}'\n")
                        cell_texts.append(safreq)
            ### text end ###

            # other open check #
            elif qtype == 'OTHER_OE' :
                for qel in qels :
                    safreq = f"dc.safreq('{qel}')"
                    py_file.write(f"{qel} = '{qel}'\n")
                    cell_texts.append(safreq)
            ### other open end ###


            if cell_texts :
                cell = '\n'.join(cell_texts)
                ipynb_cell.append(nbf.v4.new_code_cell(cell))
            else :
                mark = f'The {qid} not cotains elements'
                ipynb_cell.append(nbf.v4.new_markdown_cell(mark))

            py_file.write(f'\n')

    py_file.close()
    #ipynb_cell
    nb['cells'] = ipynb_cell
    #print(nb)
    ipynb_file_path = os.path.join(parent_path, ipynb_file_name)
    if not os.path.isfile(ipynb_file_path) :
        with open(ipynb_file_path, 'w') as f:
            nbf.write(nb, f)
    else :
        print('❗ The DataCheck ipynb file already exists')
    
    print("✅ DataCheck Setting Complete")