import pandas as pd
from pandas.api.types import is_numeric_dtype
from IPython.display import display, HTML
from typing import Union, List, Tuple, Dict, Optional, Literal, Callable, Any, NoReturn
import numpy as np
from dataclasses import dataclass, field
import contextlib
import os
import openpyxl
import re
import nbformat as nbf
from collections import OrderedDict, defaultdict
import json
from decipher.beacon import api
import time
from decipherAutomatic.key import api_key, api_server
from decipherAutomatic.getFiles import *
from decipherAutomatic.utils import *
from decipherAutomatic.dataProcessing.table import *
from pandas.io.formats import excel
import zipfile

def check_print(variables: Union[List[str], Tuple[str, ...], str], 
                error_type: Literal['SA', 'MA', 'LOGIC', 'MASA', 'MAMA', 'MARK', 'RATERANK', 'DUP'], 
                df: pd.DataFrame, 
                warnings: Optional[List[str]] = None,
                alt: Optional[str] = None) -> str:
    qid = None
    
    if isinstance(variables, str): 
        qid = variables

    if isinstance(variables, list) or isinstance(variables, tuple) :
        list_vars = list(variables)
        if len(list_vars) == 1 :
            qid = list_vars[0]
        if len(list_vars) >= 2 :
            qid = f'{list_vars[0]} - {list_vars[-1]}'
    
    error_type_msg = {
        'SA': 'SA Variable Check',
        'MA': 'MA Variable Check',
        'LOGIC': 'If Base cond is True, then Answer cond is also True',
        'DUP': 'Answer Duplicate Check',
        'MASA': 'Multi Variable Base Single Variable Check',
        'MAMA': 'Multi Variable Base Multi Variable Check',
        'MARK': 'Multi Variable Base Rank Variable Check',
        'RATERANK' : 'Check if Rank is answered in order of Rate question score'
    }


    print_str = ''
    print_str += f"""<div class="datcheck-title">📢 <span class="title-type">{error_type}</span> <span class="title-msg">({error_type_msg[error_type]})</span></div>""" # Error Text Title
    print_str += f"""👨‍👩‍👧‍👦 <span class="print-comment">Check Sample : <span class="check-bold">{len(df)}'s</span></span>"""

    # Result HTML
    correct = """<div class="datacheck-head check-correct">✅ {html_title}</div>"""
    fail    = """<div class="datacheck-head check-fail">❌ {html_title} : Error {err_cnt}'s</div>"""
    check   = """<div class="datacheck-check">📌 <span class="print-comment">{check_title}</span></div>"""
    warning  = """<div class="datacheck-warning check-warn">⚠️ {warn_title}</div>"""

    # Base Check
    err_cols = df.columns


    if warnings is not None :
        for warn in warnings :
            print_str += warning.format(warn_title=warn)

    ms_err = 'DC_BASE'
    if ms_err in err_cols :
        err_cnt = len(df[df[ms_err]==1])
        html_title = f"""Answer Base Check"""
        if err_cnt == 0 :
            print_str += correct.format(html_title=html_title)
        else :
            print_str += fail.format(html_title=html_title, err_cnt=err_cnt)

    # Cases responded to other than base 
    add_err = 'DC_NO_BASE'
    if add_err in err_cols :
        err_cnt = len(df[df[add_err]==1])
        html_title = "Other than Base Check"
        if err_cnt >= 1:
            print_str += fail.format(html_title=html_title, err_cnt=err_cnt)

    # Answer Able Value Check  / SA 
    only_err = 'ONLY_ANS'
    if only_err in err_cols :
        err_cnt = len(df[df[only_err]==1])
        html_title = "Answer Able Value Check"
        if err_cnt == 0 :
            print_str += correct.format(html_title=html_title)
        else :
            print_str += fail.format(html_title=html_title, err_cnt=err_cnt)
        
        err_answer = list(df[df[only_err]==1][variables].values)
        err_answer = ['NA' if pd.isna(x) else x for x in err_answer]
        err_answer = sorted(set(err_answer))
        if err_answer :
            print_str += f"""<div class="print-padding-left">🗒️ <span class="print-comment">Invalid response</span> : {list(err_answer)}</div>"""

    # Disable Value Check  / SA 
    isnot_err = 'ISNOT_ANS'
    if isnot_err in err_cols :
        err_cnt = len(df[df[isnot_err]==1])
        html_title = "Answer Is Not Value Check"
        if err_cnt == 0 :
            print_str += correct.format(html_title=html_title)
        else :
            print_str += fail.format(html_title=html_title, err_cnt=err_cnt)
        
        err_answer = list(set(list(df[df[isnot_err]==1][variables].values)))
        err_answer = sorted(set(err_answer))
        if err_answer :
            print_str += f"""<div class="print-padding-left">🗒️ <span class="print-comment">Invalid response</span> : {list(err_answer)}</div>"""

    # MA Variable Answer Count Check
    if (error_type in ['MA']) :
        for lg in ['DC_ATLEAST', 'DC_ATMOST', 'DC_EXACTLY'] :
            if not lg in err_cols :
                continue
            err_cnt = len(df[df[lg]==1])
            html_title = f"{lg} Check"
            if err_cnt == 0 :
                print_str += correct.format(html_title=html_title)
            else :
                print_str += fail.format(html_title=html_title, err_cnt=err_cnt)
        
        for isx in ['MA_ISIN', 'MA_ISALL', 'MA_ISNOT'] :
            if isx in list(df.columns) :
                err_cnt = len(df[df[isx]==1])
                ma, istype = isx.split('_')
                html_title = f"{ma} {istype.capitalize()} Answer Check"
                if err_cnt == 0 :
                    print_str += correct.format(html_title=html_title)
                else :
                    print_str += fail.format(html_title=html_title, err_cnt=err_cnt)

    # Answer Description Print
    desc_table = None
    if (error_type in ['SA']) and (pd.api.types.is_numeric_dtype(df[qid])) :
        desc = df[qid].describe().round(1)
        if not pd.isna(desc.loc['mean']) :
            desc_table = """
        <div class="datacheck-desc">📋 {qid} Describe</div>
        <table class="print-padding-left"">
            <tr><td><b>Count</b></td><td>{cnt}</td></tr>
            <tr><td><b>Mean</b></td><td>{mean}</td></tr>
            <tr><td><b>Min</b></td><td>{minv}</td></tr>
            <tr><td><b>Max</b></td><td>{maxv}</td></tr>
        </table>""".format(qid=qid, cnt=desc.loc['count'], mean=desc.loc['mean'], minv=desc.loc['min'], maxv=desc.loc['max'])

    if (error_type in ['MA']) :
        if 'ANSWER_CNT' in df.columns :
            desc = df['ANSWER_CNT'].describe().round(1)
            desc_table = """
        <div class="datacheck-desc">📋 {qid} Answer Count Describe</div>
        <table class="print-padding-left">
            <tr><td><b>Mean</b></td><td>{mean}</td></tr>
            <tr><td><b>Min</b></td><td>{minv}</td></tr>
            <tr><td><b>Max</b></td><td>{maxv}</td></tr>
        </table>""".format(qid=qid, mean=desc.loc['mean'], minv=desc.loc['min'], maxv=desc.loc['max'])

    if desc_table is not None :
        print_str += desc_table


    # Logic Check
    if (error_type in ['LOGIC', 'MASA', 'MAMA', 'MARK', 'RATERANK']) :
        if 'DC_LOGIC' in df.columns :
            err_cnt = len(df[df['DC_LOGIC']==1])
            base_cond = 'BASE_COND'
            if base_cond in list(df.columns) :
                base_cnt = len(df[df[base_cond]==1])
                # print_str += check.format(check_title=f"Base Cond Answer Count : <b>{base_cnt}'s</b>")
            if err_cnt == 0 :
                print_str += correct.format(html_title=f"Logic Correct")
            else :
                print_str += fail.format(html_title="Logic has Error", err_cnt=err_cnt)

    # Duplicate Check
    if (error_type in ['DUP']) :
        if 'DC_DUP' in df.columns :
            err_cnt = len(df[df['DC_DUP']==1])
            if err_cnt == 0 :
                print_str += correct.format(html_title="No Duplicate")
            else :
                dup_rows = df[df['DC_DUP'] == 1]
                summary = []
                
                for index, row in dup_rows.iterrows():
                    row_values = row[variables]
                    duplicates = row_values[row_values.duplicated(keep=False)]
                    summary.extend(duplicates.unique().tolist())
                
                summary = list(set(summary))
                print_str += fail.format(html_title=f"Duplicate Answer", err_cnt=err_cnt)
                print_str += f"""<div class="print-padding-left">🗒️ <span class="print-comment">Invalid response</span> : {summary}</div>"""


    print_type = "alt-main"
    if "check-fail" in print_str :
        print_type = "alt-fail"

    final_print = f"""
    <div class="datacheck-print {print_type}">
        <div class="datacheck-alt">{alt if alt is not None else qid}</div>
        <div class="datacheck-result">
            {print_str}
        </div>
    </div>
    """

    return final_print

def classify_variables(variable_list: List[str]):
    # 딕셔너리 생성
    classified_vars = defaultdict(list)
    # 변수명의 규칙을 추출하는 정규식
    pattern = re.compile(r"([A-Za-z]+\d*[_]?[A-Za-z]*)")
    
    for variable in variable_list:
        match = pattern.match(variable)
        if match:
            key = match.group(1)
            classified_vars[key].append(variable)
    
    return dict(classified_vars)

def get_key_id(base: List[str]) -> Union[None, str]:
    """`base`가 `qid`를 포함하는지 확인합니다."""
    qid = base[0]
    qids = list(qid)
    qids.reverse()
    for q in qids:
        if not q.isdigit():
            break
        else:
            qid = qid[:-1]

    for ma in base:
        if qid not in ma:
            qid = None
            return classify_variables(base)
        
    return qid


def lambda_ma_to_list(row, qids) :
    qid_key = get_key_id(qids)

    def return_int_or_str(txt: str) :
        rp = txt.replace(qid_key, '')
        if rp.isdigit() :
            return int(rp)
        else :
            return rp
    
    return [return_int_or_str(x) for x in qids if not (pd.isna(row[x]) or row[x] == 0)]

@dataclass
class PrintDataFrame:
    show_cols: List[str]
    df: pd.DataFrame

    def __call__(self, extra: Optional[List[str]] = None):
        if extra is None:
            return self.df[self.show_cols]
        return self.df[self.show_cols + extra] if extra else self.df

@dataclass
class ErrorDataFrame:
    chk_id: str
    qid_type: str
    show_cols: List[str]
    df: pd.DataFrame
    err_list: List[str]
    warnings: List[str] = field(default_factory=list)
    alt: Optional[str] = None

    def __post_init__(self):
        self.show_col_with_err = self.err_list + self.show_cols
        self.err_base = [x for x in self.err_list if x not in ['BASE_COND', 'ANSWER_COND']]
        self.df[self.err_list] = self.df[self.err_list].fillna(0).astype(int)
        err_df = self.df[(self.df[self.err_base]==1).any(axis=1)]
        self.err = PrintDataFrame(self.show_col_with_err, err_df)
        self.full = PrintDataFrame(self.show_col_with_err, self.df)
        self.chk_msg = check_print(self.chk_id, self.qid_type, self.full(), self.warnings, self.alt)
        self.extra_cols = []

    def __getitem__(self, key):
        extra_cols = [key] if isinstance(key, str) else key
        self.extra_cols = extra_cols
        return self.err()[self.base + extra_cols]

    def __repr__(self):
        return ''

class DataCheck(pd.DataFrame):
    _metadata = ['_keyid', '_css', '_meta_origin', '_meta', '_title', '_default_top', '_default_bottom']

    def __init__(self, *args, **kwargs):
        self._keyid = kwargs.pop('keyid', None)
        self._css = kwargs.pop('css', None)
        self._meta = kwargs.pop('meta', None)
        self._title = kwargs.pop('title', None)
        self._default_top = kwargs.pop('default_top', None)
        self._default_bottom = kwargs.pop('default_bottom', None)
        
        super().__init__(*args, **kwargs)
        if self._keyid is not None:
            self[self._keyid] = self[self._keyid].astype(int)
            self.set_index(self._keyid, inplace=True)

        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)

        self._count_fnc: Callable[[pd.Series], int] = lambda x: x.count() - (x==0).sum()
        
        self.attrs['display_msg'] = 'all'
        self.attrs['default_filter'] = pd.Series([True] * len(self), index=self.index)
        self.attrs['result_html'] = []
        self.attrs['css'] = self._css
        self.attrs['meta_origin'] = self._meta
        self.attrs['meta'] = self._meta
        self.attrs['title'] = self._title
        self.attrs['banner'] = []
        self.attrs['default_top'] = 2 if self._default_top is None else self._default_top
        self.attrs['default_bottom'] = 2 if self._default_bottom is None else self._default_bottom


    @property
    def _constructor(self) -> Callable[..., 'DataCheck']:
        return DataCheck

    def any(self, *args, **kwargs) -> pd.Series:
        """
        DataFrame의 any 메서드를 확장하여, 기본 축(axis)을 1로 설정
        """
        if 'axis' not in kwargs:
            kwargs['axis'] = 1
        return super().any(*args, **kwargs)

    def all(self, *args, **kwargs) -> pd.Series:
        """
        DataFrame의 all 메서드를 확장하여, 기본 축(axis)을 1로 설정
        """
        if 'axis' not in kwargs:
            kwargs['axis'] = 1
        return super().all(*args, **kwargs)

    @property
    def keyid(self) -> Optional[str]:
        """
        DataCheck 클래스의 keyid 속성을 반환
        """
        return self._keyid

    @keyid.setter
    def keyid(self, value: Optional[str]) -> None:
        """
        DataCheck 클래스의 keyid 속성을 설정
        """
        self._keyid = value

    @property
    def display_msg(self) -> Optional[Literal['all', 'error', None]]:
        """
        DataCheck 클래스의 display_msg 속성을 반환
        """
        return self.attrs['display_msg']

    @display_msg.setter
    def display_msg(self, option: Optional[Literal['all', 'error', None]]) -> None:
        """
        DataCheck 클래스의 display_msg 속성을 설정
        """
        if not option in ['all', 'error', None] :
            display(HTML(f"""<div class="check-bold check-fail">❌ The argument option can only be a 'all', 'error', None</div>"""))
            return
        self.attrs['display_msg'] = option

    @property
    def default_filter(self) -> pd.Series :
        """
        DataCheck 클래스의 기본 필터 조건을 반환
        """
        return self.attrs['default_filter']

    @property
    def count_fnc(self) -> Callable[[pd.Series], int]:
        """
        DataCheck 클래스의 count_fnc 속성을 반환
        """
        return self._count_fnc

    @count_fnc.setter
    def count_fnc(self, fnc: Callable[[pd.Series], int]) -> None:
        """
        DataCheck 클래스의 count_fnc 속성을 설정
        """
        if not callable(fnc):
            raise ValueError("The value must be a callable.")
        self._count_fnc = fnc

    def result_alt(self, qid: Union[str, List], alt: Optional[str]=None) -> str :
        """
        qid와 alt 값을 사용하여 결과 대체 텍스트를 생성하는 정적 메서드
        """
        alt_qid = qid
        if isinstance(qid, list) :
            alt_qid = f'{qid[0]}-{qid[-1]}'
        
        result_alt = alt_qid

        if self.attrs['title'] is not None :
            match_qid = qid
            if isinstance(qid, list) :
                match_qid = qid[0]
            
            title_dict = self.attrs['title']
            if match_qid in title_dict.keys() :
                title = title_dict[match_qid]['title']
                qtype = title_dict[match_qid]['type']
                result_alt = f'{alt_qid}: {title}'

        if alt is not None :
            result_alt = f'{alt_qid}: {alt}'

        return result_alt

    def result_html_update(self, **kwargs) :
        """
        결과 HTML을 업데이트하는 메서드로, 제공된 키워드 인수를 사용하여 기존 HTML 결과를 업데이트하거나 새로 추가
        """
        result_html = self.attrs['result_html'].copy()
        key = 'alt'
        updated = False
        if key in kwargs :
            chk_alt = {idx: result[key].strip().replace(' ', '') for idx, result in enumerate(result_html) if key in result and isinstance(result[key], str)}
            curr = kwargs[key].strip().replace(' ', '')
            for idx, value in chk_alt.items() :
                if curr == value :
                    result_html[idx] = kwargs
                    updated = True
        if not updated :
            result_html.append(kwargs)
        self.attrs['result_html'] = result_html

    def comp(self) :
        """
        전체 데이터 기준 `Series`를 Return
        """
        return pd.Series([True] * len(self), index=self.index)

    def set_filter(self, filter_cond: Optional[pd.Series] = None) -> None:
        """
        데이터 체크 기본 필터를 변경
        `filter_cond` (pd.Series or None) : `None`이면 전체 샘플 기준으로 변경
        """
        if filter_cond is None :
            self.attrs['default_filter'] = self.comp()
            if self.attrs['display_msg'] == 'all' :
                display(HTML(f"""🛠️ <span class="check-bold">Data Filter <span class="check-warn">Reset</span> : {len(self)}'s</span>"""))
        else :
            self.attrs['default_filter'] = filter_cond
            filt_data = self[filter_cond]
            if self.attrs['display_msg'] == 'all' :
                display(HTML(f"""🛠️ <span class="check-bold">Data Filter <span class="check-warn">Setting</span> : {len(filt_data)}'s</span>"""))

    def col_name_check(self, *variables: str) -> bool:
        """`qid`에 지정된 열이 데이터프레임에 있는지 확인"""
        chk_qid = [qid for qid in variables if not qid in list(self.columns)]
        if chk_qid :
            display(HTML(f"""<div class="check-bold check-fail">❌ The variable {chk_qid} is not in the data frame</div>"""))
            return False
        return True

    @contextlib.contextmanager
    def preserve_display_msg(self):
        """
        display_msg 속성을 임시로 변경하고, 코드 블록이 끝나면 원래 값으로 복원하는 컨텍스트 관리자
        """
        original_display_msg = self.attrs['display_msg']
        try:
            yield
        finally:
            self.attrs['display_msg'] = original_display_msg

    def display_description(self, alt: str, title: str, desc: pd.DataFrame=None) -> None :
        print_result = f"""
<div class="datacheck-apply alt-sub">
    <div class="datacheck-alt">{alt}</div>
    <div class="apply-title">✅ {title}</div>
</div>"""

        if desc is not None :
            desc_table = """
<table>
    <tr><td><b>Mean</b></td><td>{mean}</td></tr>
    <tr><td><b>Min</b></td><td>{minv}</td></tr>
    <tr><td><b>Max</b></td><td>{maxv}</td></tr>
</table>""".format(mean=desc.loc['mean'], minv=desc.loc['min'], maxv=desc.loc['max'])
            
            print_result = f"""
<div class="datacheck-apply alt-sub">
    <div class="datacheck-alt">{alt}</div>
    <div class="apply-title">📊 {title}</div>
    <div class="print-padding-left">{desc_table}</div>
</div>
    """
        if self.attrs['display_msg'] ==  'all' :
            display(HTML(print_result))

    def count_col(self, cnt_col_name: str, cols: Union[List[str], Tuple[str], str], value: Optional[Union[int, List[int]]] = None) -> None:
        """
        주어진 열의 응답을 세어 새로운 열을 추가하는 메서드  
        (`nan` / `0` 이 아닌 컬럼 카운트)  
        결과를 요약하여 출력  
        """
        
        cnt_col = []
        alt = ""
        if isinstance(cols, str) :
            cnt_col = [cols]
            alt = f"{cols} Answer Count"
        elif isinstance(cols, tuple) or isinstance(cols, list):
            if not self.col_name_check(*cols) : return
            cnt_col = self.ma_return(cols)
            if len(cnt_col) == 1 :
                alt = f"{cols[0]} Answer Count"
            else :
                alt = f"{cnt_col[0]}-{cnt_col[-1]} Answer Count"
        
        if value is None:
            new_col = self[cnt_col].apply(self._count_fnc, axis=1).rename(cnt_col_name)
        elif isinstance(value, int):
            new_col = self[cnt_col].apply(lambda row: row.isin([value]).sum(), axis=1).rename(cnt_col_name)
        elif isinstance(value, list):
            new_col = self[cnt_col].apply(lambda row: row.isin(value).sum(), axis=1).rename(cnt_col_name)
        if isinstance(value, range):
            value = list(value) + [value[-1] + 1]
            new_col = self[cnt_col].apply(lambda row: row.isin(value).sum(), axis=1).rename(cnt_col_name)

        with self.preserve_display_msg():
            result = self.assign(**{cnt_col_name: new_col})
            self.__dict__.update(result.__dict__)
        
        
        show_title = ""
        if value is None:
            show_title += f"""<span class="check-bold check-warn">{cnt_col_name}</span> : Answer count"""
        else:
            show_title += f"""<span class="check-bold check-warn">{cnt_col_name}</span> : Value count ({value})"""

        desc = self[cnt_col_name].describe().round(1)
        self.display_description(alt, show_title, desc)
        

    def sum_col(self, sum_col_name: str, cols: Union[List[str], Tuple[str], str]) -> None:
        """
        주어진 열의 값을 합산하여 새로운 열을 추가하는 메서드  
        결과를 요약하여 출력
        """
        if not self.col_name_check(*cols):
            return

        sum_col = []
        alt =""
        if isinstance(cols, (tuple, list)):
            sum_col = self.ma_return(cols)
            alt = f"{sum_col[0]}-{sum_col[-1]} : Sum"

        elif isinstance(cols, str):
            sum_col = [cols]
            alt = f"{cols} : Sum"

        new_col = self[sum_col].sum(axis=1).rename(sum_col_name)

        with self.preserve_display_msg():
            result = self.assign(**{sum_col_name: new_col})
            self.__dict__.update(result.__dict__)

        show_title = f"""<span class="check-bold check-warn">{sum_col_name}</span> : Sum of values"""

        desc = self[sum_col_name].describe().round(1)
        self.display_description(alt, show_title, desc)

    def ma_to_list(self, list_col_name: str, cols: Union[List[str], Tuple[str], str]) -> None:
        if not self.col_name_check(*cols):
            return

        ma_col = []
        if isinstance(cols, (tuple, list)):
            ma_col = self.ma_return(cols)
        elif isinstance(cols, str):
            ma_col = [cols]

        alt = f"{cols[0]}-{cols[-1]} : MA List" if len(cols) > 1 else f"{cols[0]} : MA List"
        new_col = self[ma_col].apply(lambda_ma_to_list, axis=1, qids=ma_col).rename(list_col_name)
        result = self.assign(**{list_col_name: new_col})
        self.__dict__.update(result.__dict__)

        show_title = f"""<span class="check-bold check-warn">{list_col_name}</span> : MA Variable to List"""

        self.display_description(alt, show_title)


    def _update_self(self, new_data):
        """
        self의 내부 데이터를 `new_data`로 업데이트
        """
        self.__dict__.update(new_data.__dict__)
     

    def ma_check(self, 
                ma: Union[List[str], Tuple[str]],
                len_chk: bool = True) -> bool:
        """
        `ma`가 리스트나 튜플인지, 그리고 다른 조건들을 만족하는지 확인
        """
        fail = """<div class="check-bold check-fail">❌ [ERROR] {warn_text}</div>"""
        example_text = """<div class="example-text">Example) {ex_text}</div>"""
        print_str = ""
        if not ma:
            print_str += fail.format(warn_text="Please check variable names")
            print_str += example_text.format(ex_text="['Q1r1', 'Q1r2', 'Q1r3'] / ('Q1r1', 'Q1r3')")
            display(HTML(print_str))
            return True

        if not isinstance(ma, (list, tuple)):
            print_str += fail.format(warn_text="Type of variable must be list or tuple")
            display(HTML(print_str))
            return True

        if len_chk and len(ma) < 2:
            print_str += fail.format(warn_text="Variable must be 2 length or more")
            display(HTML(print_str))
            return True

        if isinstance(ma, tuple) and len(ma) != 2:
            print_str += fail.format(warn_text="The variable must include 2 arguments")
            display(HTML(print_str))
            return True

        if isinstance(ma, tuple):
            cols = list(self.columns)
            first_index = cols.index(ma[0])
            last_index = cols.index(ma[1])
            if first_index > last_index:
                print_str += fail.format(warn_text=f"Please check the column index / current index ({first_index}-{last_index})")
                display(HTML(print_str))
                return True
        return False

    def ma_return(self,
                  ma: Union[List[str], Tuple[str]]) -> List[str]:
        """
        `ma`에 지정된 열을 반환
        """
        if isinstance(ma, tuple):
            cols = list(self.columns)
            first_index = cols.index(ma[0])
            last_index = cols.index(ma[1]) + 1
            return cols[first_index:last_index]
        elif isinstance(ma, list):
            return ma

    def show_message(self, 
                     export_df: ErrorDataFrame) -> None :
        """
        ErrorDataFrame 객체의 메시지를 표시하는 메서드  
        display_msg 속성에 따라 메시지를 출력
        """
        css = self.attrs['css']
        msg = export_df.chk_msg
        display_msg = """%s<br/>%s"""%(css, msg)
        if self.attrs['display_msg'] ==  'all' :
            display(HTML(display_msg))
        elif self.attrs['display_msg'] ==  'error' :
            if len(export_df.err()) > 1 :
                display(HTML(display_msg))
        elif self.attrs['display_msg'] is None :
            return


    def safreq(self, 
           qid: Optional[str] = None, 
           cond: Optional[pd.Series] = None, 
           only: Optional[Union[range, List[Union[int, float, str]], int, float, str]] = None,
           isnot: Optional[Union[range, List[Union[int, float, str]], int, float, str]] = None,
           alt: Optional[str]=None) -> 'ErrorDataFrame':
        """
        단수 응답(단일 변수) 데이터 체크 메서드
        """
        
        if not self.col_name_check(qid) : return

        show_cols = [qid]
        
        err_list = []

        # Answer Base Check
        warnings = []
        cond = (self.attrs['default_filter']) if cond is None else (self.attrs['default_filter']) & (cond)
        chk_df = self[cond].copy()

        if len(chk_df) == 0 :
            warnings.append("No response to this condition")
        else :
            ms_err = 'DC_BASE'
            filt = (chk_df[qid].isna())  # Default
            chk_df.loc[filt, ms_err] = 1

            err_list.append(ms_err)

            # ONLY ANSWER CHECK
            if only is not None:
                warnings.append(f"Only value : {only}")
                if isinstance(only, range):
                    only = list(only) + [only[-1] + 1]
                elif isinstance(only, (int, float, str)):
                    only = [only]

                only_cond = (~chk_df[qid].isin(only))
                
                only_err = 'ONLY_ANS'
                chk_df.loc[only_cond, only_err] = 1
                err_list.append(only_err)
            
            # DONT ANSWER CHECK
            if isnot is not None:
                warnings.append(f"Disable value : {isnot}")
                if isinstance(isnot, range):
                    isnot = list(isnot) + [isnot[-1] + 1]
                elif isinstance(isnot, (int, float, str)):
                    isnot = [isnot]
                
                isnot_cond = (chk_df[qid].isin(isnot))
                
                isnot_err = 'ISNOT_ANS'
                chk_df.loc[isnot_cond, isnot_err] = 1
                err_list.append(isnot_err)

            # Cases responded to other than base
            if cond is not None :
                ans_err = 'DC_NO_BASE'
                add_df = self[(self.attrs['default_filter']) & ~(cond)].copy()
                add_df = add_df[~add_df[qid].isna()].copy()
                if len(add_df) > 0 :
                    add_df[ans_err] = 1
                    err_list.append(ans_err)

                    chk_df = pd.concat([chk_df, add_df], ignore_index=True)

        set_alt = self.result_alt(qid, alt)
        edf = ErrorDataFrame(qid, 'SA', show_cols, chk_df, err_list, warnings, set_alt)
        self.show_message(edf)
        self.result_html_update(alt=set_alt, result_html=edf.chk_msg, dataframe=edf.err()[show_cols+edf.extra_cols].to_json())
        return edf

    def key_var_setting(self, cols: List, key_var: Optional[str]) -> str :
        def set_code() :
            get_qid = get_key_id(cols)
            if not isinstance(get_qid, str) :
                return get_qid
            return "%s{code}"%(get_qid)

        if key_var is None :
            return set_code()
        else :
            if not "{code}" in key_var :
                display(HTML("""<div class="datacheck-head check-fail">The `key_var` does not contain {code}.</div>"""))
                return set_code()
            return key_var

    def mafreq(self, 
            qid: Union[List[str], Tuple[str, ...]], 
            cond: Optional[pd.Series] = None, 
            atleast: Optional[int] = None, 
            atmost: Optional[int] = None, 
            exactly: Optional[int] = None,
            isin: Optional[Union[range, List[Union[int, str]], int, str]] = None,
            isall: Optional[Union[range, List[Union[int, str]], int, str]] = None,
            isnot: Optional[Union[range, List[Union[int, str]], int, str]] = None,
            no_base: bool = True,
            alt: Optional[str]=None,
            key_var: Optional[str]=None) -> 'ErrorDataFrame':
        """
        복수 응답(다중 변수) 데이터 체크 메서드
        """
        if (self.ma_check(qid)) :
            return
        
        err_list = []

        # Answer Base Check
        warnings = []
        show_cols = self.ma_return(qid)
        
        if not self.col_name_check(*show_cols) : return

        qid_key = self.key_var_setting(cols=show_cols, key_var=key_var)

        cond = (self.attrs['default_filter']) if cond is None else (self.attrs['default_filter']) & (cond)
        chk_df = self[cond].copy()

        if len(chk_df) == 0 :
            warnings.append("No response to this condition")
        else :            
            cnt = 'ANSWER_CNT'
            chk_df[cnt] = chk_df[show_cols].apply(lambda x: x.count() - (x==0).sum(), axis=1)

            ms_err = 'DC_BASE'
            # filt = (chk_df[cnt]==0)  # Default
            filt = (chk_df[show_cols].isna() | (chk_df[show_cols] == 0)).all(axis=1)
            chk_df.loc[filt, ms_err] = 1

            err_list.append(ms_err)

            # Generalized Answer Check Function
            def check_answer(condition, operator, err_label):
                if condition is not None:
                    if operator == '==':
                        cond_err = (chk_df[cnt] != condition)
                        warnings.append(f"Exactly : {condition}")
                    elif operator == '<':
                        cond_err = (chk_df[cnt] < condition)
                        warnings.append(f"Atleast : {condition}")
                    elif operator == '>':
                        cond_err = (chk_df[cnt] > condition)
                        warnings.append(f"Atmost : {condition}")
                    
                    chk_df.loc[cond_err, err_label] = 1
                    err_list.append(err_label)

            # AT LEAST, AT MOST, EXACTLY Answer Checks
            check_answer(atleast, '<', 'DC_ATLEAST')
            check_answer(atmost, '>', 'DC_ATMOST')
            check_answer(exactly, '==', 'DC_EXACTLY')

            def process_check(check_type, check_value, check_func, err_label):
                warnings.append(f"{check_type.capitalize()} value : {check_value}")
                if isinstance(check_value, range):
                    check_list = list(check_value) + [check_value[-1] + 1]
                elif isinstance(check_value, (int, str)):
                    check_list = [check_value]
                elif isinstance(check_value, list):
                    check_list = check_value

                chk_cols = [qid_key.format(code=m) for m in check_list]

                def apply_func(row):
                    return 1 if check_func(row, chk_cols) else np.nan

                chk_df[err_label] = chk_df.apply(apply_func, axis=1)

                err_list.append(err_label)

            # Check Functions
            def ma_isin_check(row, cols):
                return not any(not (pd.isna(row[c]) or row[c] == 0) for c in cols)

            def ma_isall_check(row, cols):
                return any(pd.isna(row[c]) or row[c] == 0 for c in cols)

            def ma_isnot_check(row, cols) :
                return any(not (pd.isna(row[c]) or row[c] == 0) for c in cols)

            # Is In Check
            if not isinstance(qid_key, str) :
                warnings.append("A variable structure for which the isin/isall/isnot methods are not available")
            else :
                if isin is not None:
                    process_check('isin', isin, ma_isin_check, 'MA_ISIN')

                # Is All Check
                if isall is not None:
                    process_check('isall', isall, ma_isall_check, 'MA_ISALL')

                # Is Not Check
                if isnot is not None:
                    process_check('isnot', isnot, ma_isnot_check, 'MA_ISNOT')

            # Cases responded to other than base
            if not no_base : 
                warnings.append('No Base Check does not run')
            if cond is not None and no_base :
                ans_err = 'DC_NO_BASE'
                add_df = self[self.attrs['default_filter'] & ~(cond)].copy()
                add_df[cnt] = add_df[show_cols].apply(lambda x: x.count() - (x==0).sum(), axis=1)
                add_filt = (add_df[show_cols].isna() | (add_df[show_cols] == 0)).all(axis=1)
                add_df = add_df[~add_filt].copy()
                if len(add_df) > 0 :
                    add_df[ans_err] = 1
                    err_list.append(ans_err)

                    chk_df = pd.concat([chk_df, add_df], ignore_index=True)


            show_cols = [cnt] + show_cols
        
        set_alt = self.result_alt(qid, alt)
        edf = ErrorDataFrame(qid, 'MA', show_cols, chk_df, err_list, warnings, set_alt)
        self.show_message(edf)
        self.result_html_update(alt=set_alt, result_html=edf.chk_msg, dataframe=edf.err()[show_cols+edf.extra_cols].to_json())
        return edf


    def logchk(self, 
               base: Optional[pd.Series] = None, 
               ans: pd.Series = None,
               alt: Optional[str]=None) -> 'ErrorDataFrame':
        """
        특정 로직에 대한 응답 체크
        (`base`가 `True`일 때, `ans`도 `True`)

        `base` (pd.Series): 베이스 조건.
        `ans` (pd.Series): 베이스 조건이 True일 때 응답 조건.
        """

        if ans is None :
            display(HTML("""<div class="check-bold check-fail">❌ [ERROR]  answer_cond cannot be None</div>"""))
            return 
        err_list = []

        # Base Condition Answer Check
        warnings = []
        base_cond = self.comp() if base is None else base
        base_cond = (self.attrs['default_filter']) & (base_cond)
        ans_cond  = (self.attrs['default_filter']) & (ans)
        chk_df = self[base_cond].copy()

        if len(chk_df) == 0:
            warnings.append("No response to this condition")
        else :
            # Base Filter
            base_col = 'BASE_COND'
            answer_col = 'ANSWER_COND'
            err_list += [base_col, answer_col]
            chk_df.loc[base_cond, base_col] = 1
            chk_df.loc[ans_cond, answer_col] = 1

            # Logic Check
            lg_err = 'DC_LOGIC'
            chk_df.loc[(base_cond) & (~ans_cond), lg_err] = 1
            err_list.append(lg_err)

            chk_df = chk_df[base_cond.reindex(chk_df.index, fill_value=False)]
            
        edf = ErrorDataFrame('LOGIC CHECK', 'LOGIC', [], chk_df, err_list, warnings, alt)
        self.show_message(edf)
        
        if alt is not None :
            self.result_html_update(alt=alt, result_html=edf.chk_msg, dataframe=edf.err()[edf.extra_cols].to_json())
        return edf

    def dupchk(self, 
           qid: Union[List[str], Tuple[str, ...]], 
           okUnique: Optional[Union[List[Any], range, int, str]] = None,
           alt: Optional[str]=None) -> 'ErrorDataFrame' :
        """
        중복 응답 데이터 체크 메서드 (순위 응답)        
        `qid` (Union[List[str], Tuple[str]]): 중복을 체크할 열들.
        `okUnique` (Union[List, range, int, str], optional): 무시할 특정 값(들). 기본값은 None.
        """
        if (self.ma_check(qid)) :
            return
        
        show_cols = self.ma_return(qid)
        if not self.col_name_check(*show_cols): return

        warnings = []
        err_list = []

        chk_df = self[self.attrs['default_filter']].copy()
        
        dup_err = 'DC_DUP'
        err_list.append(dup_err)

        if len(chk_df) == 0 :
            warnings.append("No response to this condition")

        if okUnique is not None:
            if not isinstance(okUnique, (list, range, int, str)):
                display(HTML("""<div class="check-bold check-fail">❌ [ERROR] Type of okUnique must be list, range, int, or str</div>"""))
                return
            if isinstance(okUnique, range):
                okUnique = list(okUnique)
                okUnique.append(okUnique[-1] + 1)
            elif isinstance(okUnique, (int, str)):
                okUnique = [okUnique]
            
            warnings.append(f"""Allow Duplicates : {okUnique}""")
        else:
            okUnique = []

        def check_duplicates(row):
            row_values = row.tolist()
            filtered_values = [value for value in row_values if value not in okUnique and not pd.isna(value)]
            return 1 if len(filtered_values) != len(set(filtered_values)) else None
        
        chk_df[dup_err] = chk_df[show_cols].apply(check_duplicates, axis=1)

        rk = show_cols
        qid = f"""{rk[0]}-{rk[-1]} (DUP)"""
        edf = ErrorDataFrame(qid, 'DUP', show_cols, chk_df, err_list, warnings, alt)
        self.show_message(edf)
        self.result_html_update(alt=self.result_alt(qid, alt), result_html=edf.chk_msg, dataframe=edf.err()[show_cols+edf.extra_cols].to_json())
        return edf

    def display_key_var_error(self, arg_name:str, qid_list: List) -> None :
            print_text = f"""<div class="check-bold check-fail">❌ [ERROR] Please check multi question variable names : `{arg_name}`</div>"""
            for key, var_list in qid_list.items() :
                print_text += f"""<div class="check-bold check-fail">[{key}] : {var_list}</div>"""
            display(HTML(print_text))

    def masa(self, 
             ma_qid: Union[List[str], Tuple[str]], 
             sa_qid: str, 
             cond: Optional[pd.Series] = None, 
             diff_value: Optional[Union[List[Any], range, int, str]] = None,
             alt: Optional[str]=None,
             key_var: Optional[str]=None) -> 'ErrorDataFrame' :
        """
        `복수 응답`을 베이스로 하는 `단수 응답` 로직 체크.
        `ma_qid` (Union[List[str], Tuple[str]]): 복수 응답 열 목록.
        `sa_qid` (str): 단수 응답 열.
        `cond` (pd.Series, optional): 조건을 적용할 시리즈. 기본값은 None.
        `diff_value` (Union[List, int], optional): 무시할 특정 값(들). 기본값은 None.
        """
        if (self.ma_check(ma_qid)) :
            return
        warnings = []
        err_list = []
         
        base_qid = self.ma_return(ma_qid)
        if not self.col_name_check(*base_qid): return
        if not self.col_name_check(sa_qid): return

        qid_key = self.key_var_setting(cols=base_qid, key_var=key_var)
        if not isinstance(qid_key, str): 
            self.display_key_var_error('ma_qid', qid_key)
            return

        cond = (self.attrs['default_filter']) if cond is None else (self.attrs['default_filter']) & (cond)
        chk_df = self[cond].copy()

        ma = base_qid
        sa = sa_qid

        show_cols = [sa] + ma

        if len(chk_df) == 0 :
            warnings.append("No response to this condition")
        else :
            filt = ~chk_df[sa].isna()

            err_col = 'DC_LOGIC'
            # MA Base SA
            if len(chk_df[filt]) == 0 :
                warnings.append("No response to this condition")

            dv = []
            if diff_value is not None:
                if not isinstance(diff_value, (list, range, int, str)):
                    display(HTML("""<div class="check-bold check-fail">❌ [ERROR] Type of diff_value must be list, range, int, or str</div>"""))
                    return
                if isinstance(diff_value, (int, str)) :
                    dv = [diff_value]
                if isinstance(diff_value, list) :
                    dv = diff_value
                if isinstance(diff_value, range):
                    dv = list(diff_value)
                    dv.append(dv[-1] + 1)
                warnings.append(f"""Do not check the code : {dv}""")
            
            def ma_base_check(x) :
                sa_ans = int(x[sa])
                if sa_ans in dv :
                    return np.nan
                
                ma_var = qid_key.format(code=sa_ans)
                ma_ans = x[ma_var]


                return 1 if pd.isna(ma_ans) or ma_ans == 0 else np.nan

            chk_df[err_col] = chk_df[filt].apply(ma_base_check, axis=1)
            err_list.append(err_col)

            ma_ans = 'BASE_MA'
            chk_df[ma_ans] = chk_df[filt].apply(lambda_ma_to_list, axis=1, qids=ma)

            show_cols = [ma_ans] + show_cols

            chk_df = chk_df if cond is None else chk_df[cond.reindex(chk_df.index, fill_value=False)]
        
        qid = f"""{sa}(SA) in {ma[0]}-{ma[-1]}(MA)"""
        edf = ErrorDataFrame(qid, 'MASA', show_cols, chk_df, err_list, warnings, alt)
        self.show_message(edf)
        self.result_html_update(alt=self.result_alt(qid, alt), result_html=edf.chk_msg, dataframe=edf.err()[show_cols+edf.extra_cols].to_json())
        return edf

    def mama(self,
             base_ma: Union[List[str], Tuple[str]], 
             chk_ma: Union[List[str], Tuple[str]], 
             cond: Optional[pd.Series] = None, 
             diff_value: Optional[Union[List[Any], range, int, str]] = None,
             alt: Optional[str]=None,
             base_key_var: Optional[str]=None,
             chk_key_var: Optional[str]=None,) -> 'ErrorDataFrame' :
        """
        `복수 응답`을 베이스로 하는 `복수 응답` 로직 체크.
        `base_ma` (Union[List[str], Tuple[str]]): 기준이 되는 복수 응답 열 목록.
        `chk_ma` (Union[List[str], Tuple[str]]): 체크할 복수 응답 열 목록.
        `cond` (pd.Series, optional): 조건을 적용할 시리즈. 기본값은 None.
        `diff_value` (Union[List, int], optional): 무시할 특정 값(들). 기본값은 None.
        """
        if (self.ma_check(base_ma)) or (self.ma_check(chk_ma)) :
            return
        warnings = []
        err_list = []
         
        base = self.ma_return(base_ma)
        chkm = self.ma_return(chk_ma)
        if not self.col_name_check(*base): return
        if not self.col_name_check(*chkm): return

        qid_key = self.key_var_setting(cols=base, key_var=base_key_var)
        ans_key = self.key_var_setting(cols=chkm, key_var=chk_key_var)

        if any(x is None for x in [qid_key, ans_key]) :
            if not isinstance(qid_key, str) : 
                self.display_key_var_error('base_ma', qid_key)

            if ans_key is None: 
                self.display_key_var_error('chk_ma', ans_key)

            return


        zip_cols = [list(x) for x in zip(base, chkm)]
        show_cols = sum(zip_cols, [])

        cond = (self.attrs['default_filter']) if cond is None else (self.attrs['default_filter']) & (cond)
        chk_df = self[cond].copy()

        if len(chk_df) == 0 :
            warnings.append("No response to this condition")
        else :
            chk_cnt = 'CHK_CNT'
            chk_df[chk_cnt] = chk_df[chkm].apply(lambda x: x.count() - (x==0).sum(), axis=1)
            filt = chk_df[chk_cnt]>=1
            
            err_col = 'DC_LOGIC'
            # MA Base MA
            if len(chk_df[filt]) == 0 :
                warnings.append("No response to this condition")

            dv = []
            diff_qids = []
            diff_ans  = []
            if diff_value is not None:
                if not isinstance(diff_value, (list, range, int, str)):
                    display(HTML("""<div class="check-bold check-fail">❌ [ERROR] Type of diff_value must be list, range, int, or str</div>"""))
                    return
                if isinstance(diff_value, (int, str)) :
                    dv = [diff_value]
                if isinstance(diff_value, list) :
                    dv = diff_value
                if isinstance(diff_value, range):
                    dv = list(diff_value)
                    dv.append(dv[-1] + 1)
                
                warnings.append(f"""Do not check the code : {dv}""")
                diff_qids = [qid_key.format(code=x) for x in dv]
                diff_ans  = [ans_key.format(code=x) for x in dv]

            def ma_base_check(x) :
                def flag(b, a) :

                    if pd.isna(b) or b == 0 :
                        if not (pd.isna(a) or a == 0) :
                            return True
                    
                    return False
                return 1 if any(flag(x[base], x[ans]) for base, ans in zip_cols if (not base in diff_qids) and (not ans in diff_ans)) else np.nan
                
            chk_df[err_col] = chk_df[filt].apply(ma_base_check, axis=1)


            def diff_ans_update(row, cols) :
                def return_int_or_str(txt: str) :
                        rp = txt.replace(qid_key.format(code=''), '')
                        if rp.isdigit() :
                            return int(rp)
                        else :
                            return rp
                return [return_int_or_str(base) for base, ans in cols if (pd.isna(row[base]) or row[base] == 0) and not (pd.isna(row[ans]) or row[ans] == 0)]

            base_ans = 'BASE_MA'
            chk_ans = 'CHECK_MA'
            diff_ans = 'DIFF_ANS'
            chk_df[base_ans] = chk_df[filt].apply(lambda_ma_to_list, axis=1, qids=base)
            chk_df[chk_ans] = chk_df[filt].apply(lambda_ma_to_list, axis=1, qids=chkm)
            chk_df[diff_ans] = chk_df[filt].apply(diff_ans_update, axis=1, cols=zip_cols)
            
            err_list.append(err_col)
            show_cols = [base_ans, chk_ans, diff_ans] + show_cols
            chk_df = chk_df if cond is None else chk_df[cond.reindex(chk_df.index, fill_value=False)]
        
        qid = f"""{chkm[0]}-{chkm[-1]}(MA) in {base[0]}-{base[-1]}(MA)"""
        edf = ErrorDataFrame(qid, 'MAMA', show_cols, chk_df, err_list, warnings, alt)
        self.show_message(edf)
        self.result_html_update(alt=self.result_alt(qid, alt), result_html=edf.chk_msg, dataframe=edf.err()[show_cols+edf.extra_cols].to_json())
        return edf

    def mark(self,
            base_qid: Union[List[str], Tuple[str]], 
            rank_qid: Union[List[str], Tuple[str]], 
            cond: Optional[pd.Series] = None, 
            diff_value: Optional[Union[List[Any], range, int, str]] = None,
            alt: Optional[str]=None,
            key_var: Optional[str]=None) -> 'ErrorDataFrame' :
        """
        `복수 응답`을 베이스로 하는 `순위 응답` 로직 체크.
        `base_qid` (Union[List[str], Tuple[str]]): 기준이 되는 복수 응답 열 목록.
        `cond` (pd.Series, optional): 조건을 적용할 시리즈. 기본값은 None.
        `rank_qid` (Union[List[str], Tuple[str]]): 체크할 순위 응답 열 목록.
        """
        if (self.ma_check(base_qid)) or (self.ma_check(rank_qid)) :
            return
        warnings = []
        err_list = []
         
        base = self.ma_return(base_qid)
        rank = self.ma_return(rank_qid)
        max_rank = len(rank)
        if not self.col_name_check(*base): return
        if not self.col_name_check(*rank): return

        qid_key = self.key_var_setting(cols=base, key_var=key_var)
        if not isinstance(qid_key, str) :
            self.display_key_var_error('base_qid', qid_key)
            return

        show_cols = rank

        cond = (self.attrs['default_filter']) if cond is None else (self.attrs['default_filter']) & (cond)
        chk_df = self[cond].copy()

        if len(chk_df) == 0 :
            warnings.append("No response to this condition")
        else :
            dv = []
            if diff_value is not None:
                if not isinstance(diff_value, (list, range, int, str)):
                    display(HTML("""<div class="check-bold check-fail">❌ [ERROR] Type of diff_value must be list, range, int, or str</div>"""))
                    return
                if isinstance(diff_value, (int, str)) :
                    dv = [diff_value]
                if isinstance(diff_value, list) :
                    dv = diff_value
                if isinstance(diff_value, range):
                    dv = list(diff_value)
                    dv.append(dv[-1] + 1)
                
                warnings.append(f"""Do not check the code : {dv}""")
                # base = [x for x in base if not x in [f'{qid_key}{d}' for d in dv]]

            base_cnt = 'BASE_COUNT'
            chk_df[base_cnt] = chk_df[base].apply(lambda x: x.count() - (x==0).sum(), axis=1)

            filt = chk_df[base_cnt]>=1

            err_col = 'DC_LOGIC'
            # MA Base MA
            if len(chk_df[filt]) == 0 :
                warnings.append("No response to this condition")

            def ma_base_rank_check(x) :
                able_ans = max_rank if x[base_cnt] > max_rank else x[base_cnt]
                chk_rank = rank[:able_ans]
                return 1 if any(pd.isna(x[rk]) for rk in chk_rank) else np.nan


            # ma_base_cond = (~chk_df[rank].isna()).any(axis=1)
            ma_base_cond = chk_df[base_cnt]>=1
            
            chk_df[err_col] = chk_df[ma_base_cond].apply(ma_base_rank_check, axis=1)

            base_ans = 'BASE_MA'
            chk_df[base_ans] = chk_df[base].apply(lambda_ma_to_list, axis=1, qids=base)


            def ma_base_check(x, rank_qid) :
                sa_ans = int(x[rank_qid])
                if sa_ans in dv :
                    return np.nan
                
                ma_var = qid_key.format(code=sa_ans)
                ma_ans = x[ma_var]


                return 1 if pd.isna(ma_ans) or ma_ans == 0 else np.nan
            # Each Rank masa
            rank_err_list = []
            for rk in rank :
                rk_err = f'{rk}_ERR'
                sa_base_cond = ~chk_df[rk].isna()
                chk_df[rk_err] = chk_df[sa_base_cond].apply(ma_base_check, axis=1, rank_qid=rk)
                rank_err_list.append(rk_err)

            masa_err = 'ERR_RK'
            def masa_rank_err(x) :
                if any(x[err]==1 for err in rank_err_list) :
                    return [cnt for cnt, rank in enumerate(rank_err_list, 1) if x[rank]==1]
                else :
                    return np.nan

            chk_df[masa_err] = chk_df[(chk_df[rank_err_list]==1).any(axis=1)].apply(masa_rank_err, axis=1)
            chk_df.loc[~chk_df[masa_err].isna(), err_col] = 1
            
            show_cols = [base_cnt, base_ans, masa_err] + rank + base
            err_list += [err_col]
            err_list += rank_err_list

            chk_df = chk_df if cond is None else chk_df[cond.reindex(chk_df.index, fill_value=False)]
        
        qid = f"""{rank[0]}-{rank[-1]}(RANK) in {base[0]}-{base[-1]}(MA)"""
        edf = ErrorDataFrame(qid, 'MARK', show_cols, chk_df, err_list, warnings, alt)
        self.show_message(edf)
        self.result_html_update(alt=self.result_alt(qid, alt), result_html=edf.chk_msg, dataframe=edf.err()[show_cols+edf.extra_cols].to_json())
        return edf


    def rate_rank(self,
                  rate_qid: Union[List[str], Tuple[str]], 
                  rank_qid: Union[List[str], Tuple[str]],
                  cond: Optional[pd.Series] = None,
                  alt: Optional[str]=None,
                  key_var: Optional[str]=None)  -> 'ErrorDataFrame' :
        """
        `척도 응답`을 베이스로 하는 `순위 응답` 로직 체크.
        ()`척도 응답`의 점수 기준으로 `순위 응답`이 순서대로 응답되어야 하는 경우)
        `rate_qid` (Union[List[str], Tuple[str]]): 기준이 되는 복수 응답 열 목록.
        `rank_qid` (Union[List[str], Tuple[str]]): 체크할 순위 응답 열 목록.
        `cond` (pd.Series, optional): 조건을 적용할 시리즈. 기본값은 None.
        """
        if (self.ma_check(rate_qid)) or (self.ma_check(rank_qid, len_chk=False)) :
            return
        warnings = []
        err_list = []
         
        rate = self.ma_return(rate_qid)
        rank = self.ma_return(rank_qid)
        if not self.col_name_check(*rate): return
        if not self.col_name_check(*rank): return

        qid_key = self.key_var_setting(cols=rate_qid, key_var=key_var)
        if not isinstance(qid_key, str) :
            self.display_key_var_error('rate_qid', qid_key)
            return

        cond = (self.attrs['default_filter']) if cond is None else (self.attrs['default_filter']) & (cond)
        chk_df = self[cond].copy()

        if len(chk_df) == 0 :
            warnings.append("No response to this condition")
        else :
            filt = (~chk_df[rank].isna()).any(axis=1)

            err_col = 'DC_LOGIC'
            def rate_rank_validate(row, rate_base, rank_base):
                scores = {int(x.replace(qid_key.format(code=''), '')): row[x] for x in rate_base if not pd.isna(row[x])}
                result = {}
                for key, value in scores.items():
                    if value not in result:
                        result[value] = []
                    result[value].append(key)
                
                sort_score = [[key, result[key]] for key in sorted(result.keys(), reverse=True)]
                
                rk = rank_base.copy()
                is_valid = False
                for sc, able in sort_score :
                    albe_rk = rk[:len(able)]
                    if not rk :
                        break
                    
                    for ar in albe_rk :
                        if not row[ar] in able :
                            is_valid = True
                        
                        rk.remove(ar)

                return 1 if is_valid else np.nan


            chk_df[err_col] = chk_df[filt].apply(rate_rank_validate, axis=1, rate_base=rate, rank_base=rank)


            def rate_rank_able_attrs(row, rate_base):
                scores = {int(x.replace(qid_key.format(code=''), '')): row[x] for x in rate_base if not pd.isna(row[x])}
                result = {}
                for key, value in scores.items():
                    if value not in result:
                        result[value] = []
                    result[value].append(key)
                
                sort_score = {int(key): result[key] for key in sorted(result.keys(), reverse=True)}
                
                return sort_score
            
            able_col = 'SCORE_ATTR'
            chk_df[able_col] = chk_df[filt].apply(rate_rank_able_attrs, axis=1, rate_base=rate)


            err_list.append(err_col)
            show_cols = [able_col] + rank + rate

            chk_df = chk_df if cond is None else chk_df[cond.reindex(chk_df.index, fill_value=False)]
        
        qid = f"""{rank[0]}-{rank[-1]}(RANK) / {rate[0]}-{rate[-1]}(RATE)"""
        edf = ErrorDataFrame(qid, 'RATERANK', show_cols, chk_df, err_list, warnings, alt)
        self.show_message(edf)
        self.result_html_update(alt=self.result_alt(qid, alt), result_html=edf.chk_msg, dataframe=edf.err()[show_cols+edf.extra_cols].to_json())
        return edf
    
    def note(self, print_word: str) -> None:
        """
        별도 표시를 위한 메서드
        """
        if self.attrs['display_msg'] ==  'all' :
            display(HTML(f"""
                         <div class="datacheck-print-mw">
                            <div class="datacheck-note-print">
                                <div class="note-title">📝 NOTE</div>
                                <div class="note-desc">{print_word}</div>
                            </div>
                         </div>
                         """))

    def live_only(self) -> None:
        """
        LIVE 상태에서 검토해야하는 부분 표기
        """
        if self.attrs['display_msg'] ==  'all' :
            display(HTML(f"""
                        <div class="datacheck-print-mw">
                            <div class="datacheck-live-check">LIVE CHECK</div>
                        </div>
                         """))

    def qset(self, qid: str, code: Union[range, List]) -> List :
        """
        `qid`와 `code`를 기준으로 `DataFrame`에서 변수명 추출
        `(startswith(qid) and endswith(each code))`
        `qid` (str) : 문자열로 된 기준 변수명 ('SQ1', 'SQ2')
        `code` (range, list) : 각 변수의 속성 코드 (`[1, 2, 3, 4]`)
        example) qid='SQ7' / code=[1, 3, 5]
        return `['SQ7r1', 'SQ7r3', 'SQ7r5']`
        """
        cols = self.columns
        if not isinstance(code, (range, list)) :
            display(HTML(f"""<div class="check-bold check-fail">❌ The argument code can only be a list or range</div>"""))
            return []

        if any(not isinstance(c, int) for c in code) :
            display(HTML(f"""<div class="check-bold check-fail">❌ The argument code can only be numeric</div>"""))
            return []
        
        chk_code = code
        if isinstance(code, range) :
            chk_code.append(chk_code[-1]+1)

        filt = [col for col in cols if col.startswith(qid) and any(str(c) in re.findall(r'\d+$', col.replace(qid, '')) for c in chk_code)]
        
        if not filt :
            display(HTML("""<div class="check-bold check-warn">⚠️ The variable does not exist in the dataframe</div>"""))
        return filt

    # DataProcessing
    def setting_meta(self, meta, variable) :
        if variable is None :
            return None

        if meta is False :
            return None

        return_meta = None
        if meta is None :
            meta_attr = self.attrs['meta']
            if meta_attr is not None :
                if isinstance(variable, str) :
                    if variable in meta_attr.keys() :
                        return_meta = meta_attr[variable]
                
                if isinstance(variable, list) :
                    return_meta = [{v: meta_attr[v]} if v in meta_attr.keys() else {v: ''} for v in variable]
        else :
            return_meta = meta
        
        return return_meta

    def setting_title(self, title, variable) :
        if variable is None :
            return None

        if title is False :
            return None

        return_title = None
        if title is None :
            title_attr = self.attrs['title']
            if title_attr is not None :
                chk_var = variable
                if isinstance(chk_var, list) :
                    chk_var = variable[0]
                
                if chk_var in title_attr.keys() :
                    set_title = title_attr[chk_var]['title']
                    set_title = set_title.replace('(HIDDEN)', '').strip()
                    return_title = set_title
        else :
            return_title = title

        return return_title

    def table(self, index: Union[str, List[str]],
                    columns: Optional[Union[str, List[str]]] = None,
                    cond: Optional[pd.Series] = None,
                    index_meta: Optional[List[Dict[str, str]]] = None,
                    columns_meta: Optional[List[Dict[str, str]]] = None,
                    include_total: bool = False,
                    index_name: Optional[str] = None,
                    columns_name: Optional[str] = None,
                    top: Optional[int] = None,
                    bottom: Optional[int] = None,
                    sort_index: Optional[str] = None) -> pd.DataFrame :

            cond = (self.attrs['default_filter']) if cond is None else (self.attrs['default_filter']) & (cond)
            df = self[cond].copy()

            index_meta = self.setting_meta(index_meta, index)
            index_name = self.setting_title(index_name, index)
            if isinstance(index, str) and isinstance(index_meta, str) :
                index_meta = None
                sort_index = "asc" if sort_index is None else sort_index

            columns_meta = self.setting_meta(columns_meta, columns)
            columns_name = self.setting_title(columns_name, columns)
            if isinstance(columns, str) and isinstance(columns_meta, str) :
                columns_meta = None
            
            result = create_crosstab(df,
                                    index=index,
                                    columns=columns,
                                    index_meta=index_meta,
                                    columns_meta=columns_meta,
                                    include_total=include_total,
                                    index_name=index_name,
                                    columns_name=columns_name,
                                    top=top,
                                    bottom=bottom,
                                    sort_index=sort_index)

            return CrossTabs(result)

    def set_banner(self, banner_list: List[Tuple]):
        # [ ('banner column name', 'banner title', banner condition) ]
        self.attrs['banner'] = []  # clear banner
        update_banner_list = self.attrs['banner']
        new_columns = {}
        new_meta = self.attrs['meta_origin']
        new_data = {}

        def add_banner_column(col, title, cond):
            if not isinstance(col, str):
                raise ValueError(f'banner column name must be string : {col}')
            
            if not isinstance(title, str):
                raise ValueError(f'banner title must be string : {title}')

            if not isinstance(cond, pd.Series):
                raise ValueError(f'banner condition must be pd.Series : {cond}')
            
            new_col = pd.Series(0, index=self.index)
            new_col[cond] = 1
            result = self.assign(**{col: new_col})
            self.__dict__.update(result.__dict__)

            new_meta[col] = title
            update_banner_list.append(col)

        for banner in banner_list:
            if isinstance(banner, tuple):
                col, title, cond = banner
                add_banner_column(col, title, cond)
            
            if isinstance(banner, list):
                group, each = banner
                if not isinstance(group, str):
                    raise ValueError(f'banner group name must be string : {banner}')
                
                if not isinstance(each, list):
                    raise ValueError(f'banner variable must be list : {banner}')
                
                for var in each:
                    col, title, cond = var
                    add_banner_column(col, title, cond)
        
        # Add all new columns to the dataframe at once
        # self.dataframe = pd.concat([self.dataframe, pd.DataFrame(new_columns, index=self.dataframe.index)], axis=1)
        self.attrs['meta'] = new_meta


    def banner_table(self, 
                    index: Union[str, List[str]],
                    cond: Optional[pd.Series] = None,
                    index_meta: Optional[List[Dict[str, str]]] = None,
                    columns_meta: Optional[List[Dict[str, str]]] = None,
                    include_total: bool = True,
                    index_name: Optional[str] = None,
                    columns_name: Optional[str] = None,
                    qtype: str = None,
                    top: Optional[int] = None,
                    bottom: Optional[int] = None,
                    sort_index: Optional[str] = None) -> pd.DataFrame :

            cond = (self.attrs['default_filter']) if cond is None else (self.attrs['default_filter']) & (cond)
            df = self[cond].copy()

            index_meta = self.setting_meta(index_meta, index)
            index_name = self.setting_title(index_name, index)
            
            if isinstance(index, str) and isinstance(index_meta, str) :
                index_meta = None
                sort_index = "asc" if sort_index is None else sort_index

            columns = self.attrs['banner']
            columns_meta = self.setting_meta(columns_meta, columns)
            columns_name = self.setting_title(columns_name, columns)

            if qtype in ['rating'] :
                # default
                top = self.attrs['default_top'] if top is None else top
                bottom = self.attrs['default_bottom'] if bottom is None else bottom
                sort_index = 'desc'

            result = create_crosstab(df,
                                    index=index,
                                    columns=columns,
                                    index_meta=index_meta,
                                    columns_meta=columns_meta,
                                    include_total=include_total,
                                    index_name=index_name,
                                    columns_name=columns_name,
                                    top=top,
                                    bottom=bottom,
                                    sort_index=sort_index)

            return CrossTabs(result)



def get_css(path: os.path) -> str:
    css_file_path = os.path.join(path)
    css = None
    try:
        with open(css_file_path, 'r') as file:
            css_content = file.read()
        css = f"""
<style>
{css_content}
</style>
"""
    except Exception as e:
        print(f"Failed to load CSS file: {e}")

    return css

def convert_columns_to_nullable_int(df):
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            # 소수점이 포함된 데이터가 있는지 확인
            if all(df[col].dropna() == df[col].dropna().astype(int)):
                df[col] = df[col].astype(pd.Int64Dtype())
            else:
                df[col] = df[col].astype(float)
    return df

def SetUpDataCheck(dataframe: pd.DataFrame, **kwargs) :
    module_path = os.path.dirname(__file__)
    css = get_css(os.path.join(module_path, 'styles.css'))
    display(HTML(css))
    df = convert_columns_to_nullable_int(dataframe)
    return DataCheck(df, css=css, **kwargs)


def DecipherDataProcessing(dataframe: pd.DataFrame, 
                           keyid: Optional[str] = "record",
                           meta_path: Optional[str] = None,
                           title_path: Optional[str] = None,
                           default_top: Optional[int] = None,
                        default_bottom: Optional[int] = None) :
    module_path = os.path.dirname(__file__)
    css_path = os.path.join(os.path.dirname(module_path), 'dataCheck')
    css = get_css(os.path.join(css_path, 'styles.css'))
    display(HTML(css))
    df = convert_columns_to_nullable_int(dataframe)

    metadata = None if meta_path is None else json.load(open(meta_path))
    title = None if title_path is None else json.load(open(title_path))
    
    return DataCheck(df, 
                     css=css, 
                     keyid=keyid,
                     meta=metadata, 
                     title=title, 
                     default_top=default_top, 
                     default_bottom=default_bottom)
    # return DataProcessing(dc, meta=metadata, title=title, default_top=default_top, default_bottom=default_bottom)


#### Decipher Ready
def unzip_and_delete(zip_path, extract_to='.'):
    """
    Function to unzip a file and delete the zip file

    Parameters:
    zip_path (str): Path to the zip file
    extract_to (str): Directory path where the contents will be extracted (default: current directory)
    """
    try:
        # Open the zip file
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Extract all the contents of the zip file to the specified directory
            zip_ref.extractall(extract_to)
        
        # Delete the zip file
        os.remove(zip_path)
    
    except FileNotFoundError:
        print(f"File not found: {zip_path}")
    
    except zipfile.BadZipFile:
        print(f"Invalid zip file: {zip_path}")
    
    except Exception as e:
        print(f"An error occurred: {e}")

def ensure_directory_exists(directory_path: str) -> None:
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

def not_empty_cell(cell, row: int) -> bool:
    """셀 값이 비어 있지 않은지 확인합니다."""
    a = cell.cell(row, 1).value
    b = cell.cell(row, 2).value
    c = cell.cell(row, 3).value

    return bool(a or b or c)

def re_big(txt: str) -> Optional[str]:
    """대괄호 안의 내용을 추출합니다."""
    re_chk = re.search(r'\[(.*?)\]', txt)
    if re_chk:
        return re_chk.group(1).strip()
    return None

def colon_split(txt: str, num: int) -> Optional[str]:
    """콜론으로 텍스트를 나누고 지정된 부분을 반환합니다."""
    re_chk = txt.split(":")
    if re_chk:
        return re_chk[num].strip()
    return None

def DecipherSetting(pid: str, 
            mode: str = 'auto', 
            cond: Optional[str] = None,
            use_variable: bool = False,
            key: str = api_key, 
            server: str = api_server, 
            meta: bool = True, 
            json_export: bool = True, 
            data_layout: bool = False, 
            base_layout: str = 'DoNotDelete',
            datamap_name: str = 'Datamap',
            mkdir: bool = False,
            dir_name: Optional[str] = None) -> None:

    """
    데이터 체크 노트북 파일 및 데이터 세팅
    
    Args:
        `pid` (str): 프로젝트 ID.
        `mode` (str, optional): 모드 ('auto' 또는 'file'). 기본값은 'auto'.
        `cond` (str, optional): 데이터 필터링 조건. 기본값은 None.
        `use_variable` (bool, optional): 변수 파일 사용 여부. 기본값은 False.
        `key` (str, optional): API 키. 기본값은 api_key.
        `server` (str, optional): API 서버. 기본값은 api_server.
        `json_export` (bool, optional): JSON 내보내기 여부. 기본값은 True.
        `data_layout` (bool, optional): 데이터 레이아웃 내보내기 여부. 기본값은 False.
        `base_layout` (str, optional): 기본 레이아웃 이름. 기본값은 'DoNotDelete'.
        `datamap_name` (str, optional): 데이터 맵 이름. 기본값은 'Datamap'.
        `mkdir` (bool, optional): 디렉토리 생성 여부. 기본값은 False.
        `dir_name` (str, optional): 디렉토리 이름. 기본값은 None.
    """

    #pd.io.formats.excel.ExcelFormatter.header_style = None
    excel.ExcelFormatter.header_style = None
    
    if pid == '' or not pid :
        print('❌ Please enter pid')
        return

    if not mode in ['auto', 'file'] :
        print('❌ Please check the mode argument (auto or file)')
        return

    parent_path = os.getcwd()
    if mkdir :
        folder_name = pid
        if dir_name != None :
            folder_name = dir_name
        parent_path =  os.path.join(parent_path, folder_name)
        chk_mkdir(parent_path)


    if mode == 'file' :
        file_name = f'{pid}.xlsx'
        xl = openpyxl.load_workbook(file_name)
        map_sheet = datamap_name
        data_map = xl[map_sheet]
        print('📢 Read excel file (xlsx)')

    if mode == 'auto' :
        file_name = f'{pid}.csv'
        if cond != None :
            if cond.isdigit() :
                print('❌ [ERROR] : The cond argument can only be a string')
                return
        delivery_cond = 'qualified' if cond == None else f'qualified and {cond}'
        try :
            api.login(key, server)
        except :
            print('❌ Error : Decipher api login failed')
            return

        path = f'surveys/selfserve/548/{pid}'
        # get csv data
        try :
            csv_data = api.get(f'{path}/data', format='csv', cond=delivery_cond)
            sav_data = api.get(f'{path}/data', format='spss16', cond=delivery_cond)
        except :
            print('❌ Error : Please check the cond argument')
            return

        csv_binary = f'binary_{pid}.csv'
        data_path = os.path.join(parent_path, 'data')
        ensure_directory_exists(data_path)
        create_binary_file(data_path, csv_binary, csv_data)
        create_ascii_file(data_path, csv_binary, f'{pid}.csv')
        
        sav_zip = f'{pid}_sav.zip'
        create_binary_file(data_path, sav_zip, sav_data)
        unzip_and_delete(os.path.join(data_path, sav_zip), data_path)
        time.sleep(3)

        # get datamap xlsx

        map_xlsx = api.get(f'{path}/datamap', format='xlsx')
        
        map_path = os.path.join(parent_path, 'map')
        ensure_directory_exists(map_path)
        create_binary_file(map_path, f'mapsheet_{pid}.xlsx', map_xlsx)

        xl = openpyxl.load_workbook(os.path.join(map_path, f'mapsheet_{pid}.xlsx'))
        map_sheet = 'datamap'
        data_map = xl[map_sheet]

        print('📢 Using Decipher REST API')

    mx_row = data_map.max_row
    mx_col = data_map.max_column

    key_ids = key_vars
    diff_vars = sys_vars
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
            na_name = el.split('_')[:-1]
            na_el = '_'.join(na_name).replace(na, '')
            if not na_el == '' :
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
        with open(os.path.join(map_path, f'map_{pid}.json'), 'w', encoding='utf-8') as f :
            json.dump(qids, f, ensure_ascii=False, indent=4)

    if meta :
        meta_path = os.path.join(parent_path, 'meta')
        ensure_directory_exists(meta_path)
        metadata = decipher_meta(pid)
        title = decipher_title(pid)

        with open(os.path.join(meta_path, f'meta_{pid}.json'), 'w', encoding='utf-8') as f :
            json.dump(metadata, f, ensure_ascii=False, indent=4)
        
        with open(os.path.join(meta_path, f'title_{pid}.json'), 'w', encoding='utf-8') as f :
            json.dump(title, f, ensure_ascii=False, indent=4)

    # print(qids)
    # data layout export
    if data_layout :
        try :
            api.login(key, server)
            survey = f'selfserve/548/{pid}'
            url = f'surveys/{survey}/layouts'
            map = api.get(url)
        except :
            print('❌ Error : Decipher API failed')
        
        maps = [m for m in map if m['description'] == base_layout ]
        if not maps :
            print(f'❌ Error : The base layout({base_layout}) is null')
            return 
        base_map = maps[0]

        variables = base_map['variables']
        # print(variables)
        exactly_diff_vars = key_vars + sys_vars
        ce_vars = ['radio', 'checkbox', 'number', 'float', 'select']
        oe_vars = ['text', 'textarea']
        diff_label_names = ['vqtable', 'voqtable', 'dummy', 'DUMMY', 'Dummmy']
        
        layout_path = os.path.join(parent_path, 'layout')
        ensure_directory_exists(layout_path)
        ce = open(os.path.join(layout_path, f'CE_{pid}.txt'), 'w')
        oe = open(os.path.join(layout_path, f'OE_{pid}.txt'), 'w')

        for label, width in [ ('record', 7), ('uuid', 16), ('UID', 16)]:
            write_text = f'{label},{label},{width}\n'
            ce.write(write_text)
            oe.write(write_text)

        resp_chk = [v for v in variables if v['label'] == 'RespStatus']
        if resp_chk :
            ce.write(f'RespStatus,RespStatus,8\n')

        for var in variables :
            label = var['label']
            qlabel = var['qlabel']
            qtype = var['qtype']
            fwidth = var['fwidth']
            altlabel = var['altlabel']
            shown = var['shown']
            if not shown :
                continue

            write_text = f'{label},{altlabel},{fwidth}\n'
            if (not label in exactly_diff_vars and not qlabel in exactly_diff_vars) :
                if [dl for dl in diff_label_names if (dl in label) or (dl in qlabel)] :
                    continue
                if qtype in ce_vars :
                    if qtype in ['number', 'float'] :
                        verify_check = [attr['value'].split('-')[1] for ql, attr in list(qids.items()) if (ql == qlabel) or (ql == label)]
                        if verify_check :
                            max_width = len(verify_check[0])
                                # print(label, verify_check, max_width)
                            if qtype == 'float' :
                                max_width += 4
                            write_text = f'{label},{altlabel},{max_width}\n'
                    ce.write(write_text)
                if qtype in oe_vars :
                    oe.write(write_text)

        oe.write(f'decLang,decLang,60\n')
        # if resp_chk :
        #     oe.write(f'RespStatus,RespStatus,8\n')

        ce.close()
        oe.close()

    # variable py file create
    variable_py_name = f'variables_{pid}.py'
    py_file = open(os.path.join(map_path, variable_py_name), 'w')
    py_file.write(f'# {pid} variables\n')

    ipynb_cell = []

    # set_file_name = 'pd.read_excel(file_name)' if mode == 'file' else 'pd.read_csv(file_name, low_memory=False)'

    excel_meta = f'''DecipherDataProcessing(df, meta_path="meta/meta_{pid}.json", title_path="meta/title_{pid}.json")''' if meta else '''DecipherDataProcessing(df)'''

    default = f'''import pandas as pd
import pyreadstat
import numpy as np
from map.variables_{pid} import * 
from decipherAutomatic.dataProcessing.dataCheck import *

pid = "{pid}"

# Use SPSS
# file_name = f"data/{{pid}}.sav"
# df, meta = pyreadstat.read_sav(file_name)
# df = DecipherDataProcessing(df)

# Use Excel
file_name = f"data/{{pid}}.xlsx"
df = pd.read_excel(file_name, engine="openpyxl")
df = {excel_meta}
'''
    
    ipynb_cell.append(nbf.v4.new_code_cell(default))
    ipynb_cell.append(nbf.v4.new_code_cell("""# df.display_msg = 'error'"""))

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

                    if len(qels) >= 2 :
                        diff_na = [q for q in qels if not na in q]
                        py_file.write(f"{qid} = {diff_na}\n")

                    for qel in qels :
                        if na in qel :
                            cell_texts.append(f'# The {qid} contains {qel}')
                        else :
                            safreq = f"df.safreq('{qel}')"
                            if use_variable : safreq = f"df.safreq({qel})"

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
                            dupchk = f"df.dupchk({set_qid})"
                            if use_variable : dupchk = f"df.dupchk({qid})"

                            cell_texts.append(dupchk)
                else :
                    if qval :
                        val_chk = f"# value : {qval}"
                        py_file.write(f"{qid} = '{qid}'\n")
                        py_file.write(f'{qid}_value = [0, 1]\n')
                        
                        cell_texts.append(val_chk)
                        safreq = f"df.safreq('{qels[0]}')"
                        if use_variable : safreq = f"df.safreq({qels[0]})"
                        cell_texts.append(safreq)
            ### sa end ###

            # ma check #
            elif qtype == 'MA' :
                if len(qels) > 1 :
                    diff_na = [q for q in qels if not na in q]
                    if not diff_na :
                        continue
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

                    mafreq = f"df.mafreq({set_qid})"
                    if use_variable : mafreq = f"df.mafreq({qid})"

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
                            safreq = f"df.safreq('{qel}', {range_set})"
                            if use_variable : safreq = f"df.safreq({qel}, {range_set})"
                        else :
                            safreq = f"df.safreq('{qel}')"
                            if use_variable : safreq = f"df.safreq({qel})"

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
                        safreq = f"df.safreq('{qel}')"
                        if use_variable : safreq = f"df.safreq({qel})"

                        py_file.write(f"{qel} = '{qel}'\n")
                        cell_texts.append(safreq)
            ### text end ###

            # other open check #
            elif qtype == 'OTHER_OE' :
                for qel in qels :
                    safreq = f"df.safreq('{qel}')"
                    if use_variable : safreq = f"df.safreq({qel})"

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