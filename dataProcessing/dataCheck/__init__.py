import pandas as pd
from pandas.api.types import is_numeric_dtype
from IPython.display import display, HTML
from typing import Union, List, Tuple, Dict, Optional, Literal, Callable, Any, TypedDict
import numpy as np
from dataclasses import dataclass, field
import contextlib
import os
import openpyxl
from openpyxl import load_workbook
from openpyxl.styles import Alignment
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
from matplotlib.colors import to_hex
from matplotlib.colors import LinearSegmentedColormap
from pprint import pprint

VarWithHeader = Tuple[str, Union[str, List[str]]]
ColumnsWithHeader = List[VarWithHeader]
IndexWithTypes = Union[str, List[str], Tuple[str]]

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

def calculate_bg_color(value):
    cmap = LinearSegmentedColormap.from_list("custom_blue", ["#ffffff", "#2d6df6"])
    normalized_value = value / 100  # Normalize the value between 0 and 1
    return to_hex(cmap(normalized_value))

def check_duplicate_meta(input_list: List[Dict[str, str]]) -> List[Dict[str, str]]:
    value_counts = {}
    for item in input_list:
        for key, value in item.items():
            if value in value_counts:
                value_counts[value] += 1
            else:
                value_counts[value] = 1
    
    value_indices = {value: 1 for value in value_counts}
    output_list = []

    for item in input_list:
        for key, value in item.items():
            if value_counts[value] > 1:
                new_value = f"{value_indices[value]}. {value}"
                value_indices[value] += 1
            else:
                new_value = value
            output_list.append({key: new_value})

    return output_list

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
        self.df[self.err_list] = self.df[self.err_list].where(self.df[self.err_list].notna(), 0).astype(int)
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
    _metadata = ['_keyid', '_css', '_meta_origin', '_meta', '_title', '_default_top', '_default_bottom', '_default_medium', '_default_with_value']
    def __init__(self, *args, **kwargs):
        self._keyid = kwargs.pop('keyid', None)
        self._css = kwargs.pop('css', None)
        self._meta = kwargs.pop('meta', None)
        self._title = kwargs.pop('title', None)
        self._default_top = kwargs.pop('default_top', None)
        self._default_bottom = kwargs.pop('default_bottom', None)
        self._default_medium = kwargs.pop('default_medium', None)
        self._default_with_value = kwargs.pop('default_with_value', None)
        
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
        self.attrs['meta_origin'] = self._meta if self._meta is not None else {}
        self.attrs['meta'] = self._meta if self._meta is not None else {}
        self.attrs['title_origin'] = self._title if self._title is not None else {}
        self.attrs['title'] = self._title if self._title is not None else {}
        self.attrs['default_top'] = 2 if self._default_top is None else self._default_top
        self.attrs['default_bottom'] = 2 if self._default_bottom is None else self._default_bottom
        self.attrs['default_medium'] = True if self._default_medium is None else self._default_medium
        self.attrs['default_with_value'] = False if self._default_with_value is None else self._default_with_value
        self.attrs['nets'] = {}
        self.attrs['banner'] = None
        self.attrs['proc_result'] = OrderedDict()


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
                vgroup = title_dict[match_qid]['vgroup']
                title = title_dict[vgroup]['title']
                qtype = title_dict[vgroup]['type']

                if not qtype in ['multiple'] :
                    sub_title = title_dict[match_qid]['sub_title']

                    if sub_title is not None :
                        result_alt = f'{alt_qid}: {title}_{sub_title}'
                    else :
                        result_alt = f'{alt_qid}: {title}'
                else :
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

    def count_col(self, cnt_col_name: str, cols: Union[List[str], Tuple[str], str], value: Optional[Union[int, List[int], range]] = None) -> None:
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
        elif isinstance(value, range):
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

        def no_base_check() :
            if cond is not None :
                ans_err = 'DC_NO_BASE'
                add_df = self[(self.attrs['default_filter']) & ~(cond)].copy()
                add_df = add_df[~add_df[qid].isna()].copy()
                if len(add_df) > 0 :
                    add_df[ans_err] = 1
                    err_list = [ans_err]
                    chk_df = add_df
                    return [chk_df, err_list]
                else :
                    return None
            else :
                return None

        if len(chk_df) == 0 :
            warnings.append("No response to this condition")
            no_base = no_base_check()
            if no_base is not None :
                chk_df, err_list = no_base
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
            no_base = no_base_check()
            if no_base is not None :
                chk_df, err_list = no_base
        
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

        def no_base_check() :
            if cond is not None and no_base :
                ans_err = 'DC_NO_BASE'
                add_df = self[self.attrs['default_filter'] & ~(cond)].copy()
                add_df[cnt] = add_df[show_cols].apply(lambda x: x.count() - (x==0).sum(), axis=1)
                add_filt = (add_df[show_cols].isna() | (add_df[show_cols] == 0)).all(axis=1)
                add_df = add_df[~add_filt].copy()
                if len(add_df) > 0 :
                    add_df[ans_err] = 1
                    err_list = [ans_err]
                    chk_df = add_df
                    return [chk_df, err_list]
                else :
                    return None
            else :
                return None

        if len(chk_df) == 0 :
            warnings.append("No response to this condition")
            no_base = no_base_check()
            if no_base is not None :
                chk_df, err_list = no_base
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
                no_base = no_base_check()
                if no_base is not None :
                    chk_df, err_list = no_base


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
           cond: Optional[pd.Series] = None, 
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

        cond = (self.attrs['default_filter']) if cond is None else (self.attrs['default_filter']) & (cond)
        chk_df = self[cond].copy()
        
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
        alt = f"""{rk[0]}-{rk[-1]} (DUP)"""
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
             ma_qid: Union[List[str], Tuple[str, ...]], 
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
             base_ma: Union[List[str], Tuple[str, ...]], 
             chk_ma: Union[List[str], Tuple[str, ...]], 
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
            base_qid: Union[List[str], Tuple[str, ...]], 
            rank_qid: Union[List[str], Tuple[str, ...]], 
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
                  rate_qid: Union[List[str], Tuple[str, ...]], 
                  rank_qid: Union[List[str], Tuple[str, ...]],
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
    def setting_meta(self, meta, variable, dup_chk=True) :
        if variable is None :
            return None

        if meta is False :
            return None

        return_meta = None
        if meta is None :
            meta_attr = self.attrs['meta']
            if meta_attr :
                if isinstance(variable, str) :
                    if variable in meta_attr.keys() :
                        return_meta = meta_attr[variable]
                
                if isinstance(variable, list) :
                    return_meta = [{v: meta_attr[v]} if v in meta_attr.keys() else {v: v} for v in variable]
            else :
                return None
        else :
            return_meta = meta
        
        if return_meta is not None and dup_chk :
            return_meta = check_duplicate_meta(return_meta)

        return return_meta

    def setting_title(self, title, variable) :
        if variable is None :
            return None

        if title is False :
            return None

        return_title = None
        if title is None :
            title_attr = self.attrs['title']
            if title_attr :
                chk_var = variable
                if isinstance(chk_var, list) :
                    chk_var = variable[0]
            
                if chk_var in title_attr.keys() :
                    set_title = ''
                    qtype = None
                    if 'vgroup' in title_attr.keys() :
                        vgroup = title_attr[chk_var]['vgroup']
                        set_title = title_attr[vgroup]['title']
                        qtype = title_attr[vgroup]['type']
                    else :
                        set_title = title_attr[chk_var]['title']
                        qtype = title_attr[chk_var]['type']
                    
                    set_title = set_title.replace('(HIDDEN)', '').strip()

                    if not qtype in ['multiple'] :
                        sub_title = title_attr[chk_var]['sub_title']

                        if sub_title is not None :
                            set_title = f'{set_title}_{sub_title}'

                    return_title = set_title
            else :
                return None
        else :
            return_title = title

        return return_title

    def table(self, index: Union[str, List[str]],
                    columns: Optional[Union[str, List[str]]] = None,
                    cond: Optional[pd.Series] = None,
                    index_meta: Optional[List[Dict[str, str]]] = None,
                    columns_meta: Optional[List[Dict[str, str]]] = None,
                    index_filter: Optional[List[Union[str, int]]] = None,
                    columns_filter: Optional[List[Union[str, int]]] = None,
                    index_sort: Optional[Literal['asc', 'desc']]=None,
                    columns_sort: Optional[Literal['asc', 'desc']]=None,
                    fill: bool = True,
                    qtype: Optional[Literal['single', 'rating', 'rank', 'multiple', 'number', 'text']] = None,
                    score: Optional[int] = None,
                    top: Optional[int] = None,
                    medium: Optional[Union[int, List[int], bool]] = None,
                    bottom: Optional[int] = None,
                    reverse_rating: Optional[bool]=False,
                    aggfunc: Optional[list] = None,
                    with_value: bool = None,
                    group_name: Optional[str] = None,
                    base_desc: Optional[str] = None) -> pd.DataFrame :

            cond = (self.attrs['default_filter']) if cond is None else (self.attrs['default_filter']) & (cond)
            df = self[cond].copy()

            with_value = self.attrs['default_with_value'] if with_value is None else with_value

            filt_variables = []

            # if isinstance(index, tuple) :
            #     raise NotImplementedError('tuple index is not supported')
            
            # if isinstance(columns, tuple) :
            #     raise NotImplementedError('tuple columns is not supported')

            if isinstance(index, tuple) :
                index = self.ma_return(index)
                if not self.col_name_check(*index) : return

            if isinstance(columns, tuple) :
                columns = self.ma_return(columns)
                if not self.col_name_check(*columns) : return


            # Table Header
            varable_text = []
            if isinstance(index, list) :
                varable_text.append(f'{index[0]}-{index[-1]}')
            else :
                varable_text.append(index)

            if isinstance(columns, list) :
                varable_text.append(f'{columns[0]}-{columns[-1]}')
            else :
                if columns is not None :
                    varable_text.append(columns)

            # Index
            if isinstance(index, (list)) :
                # index = self.ma_return(index)
                # if not self.col_name_check(*index) : return
                filt_variables += index
            else :
                filt_variables.append(index)

            # Columns
            if isinstance(columns, (list)) :
                # columns = self.ma_return(columns)
                # if not self.col_name_check(*columns) : return
                filt_variables += columns
            else :
                if columns is not None :
                    filt_variables.append(columns)

            filt_variables = list(set(filt_variables))

            df = df[filt_variables].copy()

            original_index_meta = index_meta
            original_columns_meta = columns_meta

            titles = self.attrs['title']
            metas = self.attrs['meta']

            if qtype is None :
                if isinstance(index, str) :
                    if index in titles.keys() :
                        qtype = titles[index]['type']

                if isinstance(index, list) :
                    if index[0] in titles.keys() :
                        qtype = titles[index[0]]['type']

            index_meta = self.setting_meta(original_index_meta, index, not qtype in ['rank'])
            if index_filter is not None :
                if index_meta is not None :
                    index_meta_dict = {list(idx.keys())[0]:list(idx.values())[0] for idx in index_meta}
                    index_meta = [{str(i) : index_meta_dict[str(i)]} for i in index_filter if str(i) in index_meta_dict.keys()]
                else :
                    index_meta = [{str(i) : i} for i in index_filter]

            if isinstance(index, str) and isinstance(index_meta, str) :
                index_meta = None

            if index_meta is not None and index_sort is not None :
                if index_sort == 'asc' :
                    index_meta = sorted(index_meta, key=lambda d: list(d.keys())[0])
                
                if index_sort == 'desc' :
                    index_meta = sorted(index_meta, key=lambda d: list(d.keys())[0], reverse=True)

            columns_meta = self.setting_meta(original_columns_meta, columns, dup_chk=False)
            if columns_filter is not None :
                if columns_meta is not None :
                    columns_meta_dict = {list(col.keys())[0]:list(col.values())[0] for col in columns_meta}
                    columns_meta = [{str(i) : columns_meta_dict[str(i)]} for i in columns_filter]
                else :
                    columns_meta = [{str(i) : i} for i in columns_filter]


            if isinstance(columns, str) and isinstance(columns_meta, str) :
                columns_meta = None

            if columns_meta is not None and columns_sort is not None :
                if columns_sort == 'asc' :
                    columns_meta = sorted(columns_meta, key=lambda d: list(d.keys())[0])
                
                if columns_sort == 'desc' :
                    columns_meta = sorted(columns_meta, key=lambda d: list(d.keys())[0], reverse=True)

 
            # Number Type
            if qtype == 'number' :
                if not isinstance(index, str) :
                    raise TypeError("index must be str")
                
                if aggfunc is None :
                    aggfunc = ['mean', 'min', 'max']

            # Rating Type
            if qtype == 'rating' :
                if not isinstance(index, str) :
                    raise TypeError("index must be str")
                
                top = self.attrs['default_top'] if top is None else top
                bottom = self.attrs['default_bottom'] if bottom is None else bottom
                medium = self.attrs['default_medium'] if medium is None else medium
                
                if aggfunc is None :
                    aggfunc = ['mean']
                
                if score is None :
                    answers = max(df[index].value_counts().index.tolist())
                    score = answers
            
            # Rank Type (Grid Rank)
            if qtype == 'rank' :
                if not isinstance(index, list) :
                    raise TypeError("index must be list")
                
                answers = []
                
                index_meta = None
                if titles :
                    vgroup = list(set([titles[i]['vgroup'] for i in index if i in titles.keys()]))
                    if vgroup :
                        if len(vgroup) != 1 :
                            raise ValueError("The elements in the index must be in the same vgroup")
                                                
                        if vgroup[0] in metas.keys() :
                            index_meta = metas[vgroup[0]]
                
                if index_meta is None :
                    for idx in index :
                        if not isinstance(idx, str) :
                            raise TypeError("The elements in the index must be strings")

                        answer = df[idx].value_counts().index.to_list()
                        answers += answer
                    
                    answers = list(set(answers))
                else :
                    answers = [int(list(idx.keys())[0]) for idx in index_meta]
                
                rank_df = df.copy()

                # Grid Sinlge Rank to Multi Variable
                rank_index = []
                rank_index_meta = []
                
                for ans in answers :
                    set_var = f'#C{ans}'
                    rank_df.loc[:, set_var] = 0
                    rank_df.loc[(rank_df[index]==ans).any(axis=1), set_var] = 1

                    rank_index.append(set_var)
                    if index_meta is None :
                        if original_index_meta is not None :
                            set_label = [list(i.values())[0] for i in original_index_meta if list(i.keys())[0] == ans]
                            if set_label :
                                rank_index_meta.append({set_var: set_label[0]})
                        else :
                            rank_index_meta.append({set_var: ans})
                    else :
                        meta_dict = {list(idx.keys())[0]: list(idx.values())[0] for idx in index_meta}
                        rank_index_meta.append({set_var: meta_dict[str(ans)]})
                
                index = rank_index
                index_meta = rank_index_meta
                df = rank_df

            
            # With Value
            if index_meta is not None and with_value :
                new_index_meta = []
                for idx in index_meta :
                    new_meta = {}
                    for k, m in idx.items() :
                        new_meta[k] = f'[{k}] {m}'
                    new_index_meta.append(new_meta)
                
                index_meta = new_index_meta
            
            total_label ='Total'
            
            result = create_crosstab(df,
                                    index=index,
                                    columns=columns,
                                    index_meta=index_meta,
                                    columns_meta=columns_meta,
                                    qtype=qtype,
                                    score=score,
                                    fill=fill,
                                    top=top,
                                    medium=medium,
                                    bottom=bottom,
                                    aggfunc=aggfunc,
                                    reverse_rating=reverse_rating, 
                                    total_label=total_label)
            
            result = CrossTabs(result)
            result.attrs['type'] = qtype

            
            if base_desc is None :
                sample_count = len(self)
                all_count = int(result.iloc[0, 0])
                
                if sample_count == all_count :
                    base_desc = 'All Base'
                else :
                    sample_ratio = round(all_count/sample_count, 2) * 100
                    base_desc = f'Not All Base ({sample_ratio:.0f}%)'

            result.index = pd.MultiIndex.from_tuples([('' if group_name is None else group_name, i) for i in result.index])
            result.index.names = pd.Index(['/'.join(varable_text), base_desc])

            return result

    def netting(self, banner_list: List[Union[Tuple, List]]):
        # [ ('banner column name', 'banner title', banner condition) ]
        new_meta = self.attrs['meta_origin']
        new_title = self.attrs['title_origin']
        nets = self.attrs['nets']
        
        def add_netting_column(col, title, cond, vgroup=None):
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
            new_title[col] = {
                'type': 'multiple',
                'title': title,
                'sub_title': None,
                'vgroup': vgroup,
            }
            if vgroup is None :
                nets[col] = col

        def add_netting_group(group_label, group_title, banners) :
            new_meta[group_label] = group_title
            
            banner_child = []
            col_group = []
            
            new_title[group_label] = {
                'type': 'multiple',
                'title': group_title,
                'sub_title': None,
                'vgroup': None,
            }

            for ba in banners :
                col, title, cond = ba
                add_netting_column(col, title, cond, group_label)
                banner_child.append(col)
                col_group.append(col)

            nets[group_label] = col_group


        for banner in banner_list:
            if isinstance(banner, tuple) :
                col, title, cond = banner
                add_netting_column(col, title, cond)
            
            if isinstance(banner, list):
                group, bas = banner
                if not isinstance(group, tuple):
                    raise ValueError(f'banner group must be tuple : {banner}')
                
                if not isinstance(bas, list):
                    raise ValueError(f'banner variable must be list : {banner}')
                
                glabel, gtitle = group
                add_netting_group(glabel, gtitle, bas)
                
        # Add all new columns to the dataframe at once
        # self.dataframe = pd.concat([self.dataframe, pd.DataFrame(new_columns, index=self.dataframe.index)], axis=1)
        self.attrs['meta'] = new_meta
        self.attrs['title'] = new_title
        self.attrs['nets'] = nets

        # 데이터 저장을 위한 리스트 초기화
        data = []

        for key, value in nets.items():
            if isinstance(value, str):
                meta = self.attrs['meta'][value]
                sample = (self[value] == 1).sum()
                data.append([None, None, value, meta, sample])
            if isinstance(value, list):
                for v in value:
                    meta = self.attrs['meta'][v]
                    sample = (self[v] == 1).sum()
                    title = self.attrs['meta'][key]
                    data.append([key, title, v, meta, sample])

        # 리스트를 DataFrame으로 변환
        df = pd.DataFrame(data, columns=["Group Variable", "Group Title", "Variable", "Title", "Sample"])
        return df

#         show_nets = f"""<table>
#     <thead>
#         <th>Group Varialbe</th>
#         <th>Group Title</th>
#         <th>Variable</th>
#         <th>Title</th>
#         <th>Sample</th>
#     </thead>
# """
#         for key, value in nets.items():
#             if isinstance(value, str):
#                 meta = self.attrs['meta'][value]
#                 sample = (self[value]==1).sum()
#                 show_nets += f"""<tr><td></td><td></td><td>{value}</td><td>{meta}</td><td>{sample}'s</td></tr>"""
#             if isinstance(value, list):
#                 for v in value :
#                     meta = self.attrs['meta'][v]
#                     sample = (self[v]==1).sum()
#                     title = self.attrs['meta'][key]
#                     show_nets += f"""<tr><td>{key}</td><td>{title}</td><td>{v}</td><td>{meta}</td><td>{sample}'s</td></tr>"""
#         show_nets += """</table>"""

#         display(HTML(show_nets))


    def net(self, key: Optional[str] = None) :
        nets = self.attrs['nets']
        if key is None :
            if nets :
                return [(key, var) for key, var in nets.items()]
            else :
                return None
        
        if not isinstance(key, str):
            raise ValueError(f'key must be str : {key}')
        
        if key not in nets.keys():
            raise ValueError(f'key not found in nets : {key}')

        return nets[key]

    def set_banner(self, banner_list: Optional[List[Tuple[str, List]]] = None):
        self.attrs['banner'] = banner_list
        

    def proc(self, 
             index: IndexWithTypes, 
             columns: Optional[ColumnsWithHeader] = None, 
             fill: bool = True, 
             group_name: Optional[str] = None,
             base_desc: Optional[str] = None,
             **options) :
        merge_result = None

        if columns is None :
            banner = self.attrs['banner']
            if banner is None :
                raise ValueError("banner is not set")
            else :
                columns = banner

        titles = self.attrs['title']

        if not isinstance(columns, list) :
            raise TypeError("columns must be a list")

        index_name = index
        if isinstance(index, tuple) :
            index = self.ma_return(index)
            if not self.col_name_check(*index) : return

        if isinstance(index, list) :
            if len(index) == 1 :
                index_name = index[0]
            else :
                index_name = f'{index[0]}-{index[-1]}'

        tables = []
        for col_head, col in columns :
            header = col_head
            if col_head in titles.keys() :
                header = titles[col_head]['title']
            
            tables.append((header, self.table(index, col, **options)))

        merge_table = pd.concat([t for head, t in tables], axis=1)
        new_columns = []
        qtypes = []
        for head, table in tables :
            new_columns.append(('', table.columns[0]))
            qtypes.append(table.attrs['type'])
            for col in table.columns[1:] : # Total 제외
                # From Tuple
                new_columns.append((head, col))
        
        merge_table.columns = pd.MultiIndex.from_tuples(new_columns)
        merge_result = merge_table.loc[:, ~merge_table.columns.duplicated()]

        if isinstance(merge_result.index, pd.MultiIndex) :
            merge_result.index = merge_result.index.droplevel(0)

        merge_result.index = pd.MultiIndex.from_tuples([('' if group_name is None else group_name, idx) for idx in merge_result.index])
        
        if base_desc is None :
            sample_count = len(self)
            all_count = int(merge_result.iloc[0, 0])
            if sample_count == all_count :
                base_desc = 'All Base'
            else :
                sample_ratio = round(all_count/sample_count, 2) * 100
                base_desc = f'Not All Base ({sample_ratio:.0f}%)'

        merge_result.index.names = pd.Index([index_name, base_desc])

        # merge_result = merge_result.fillna('-')
        if not fill :
            merge_result = merge_result.loc[(merge_result != 0).any(axis=1), (merge_result != 0).any(axis=0)]
            #merge_result = merge_result.loc[(merge_result != '-').any(axis=1), (merge_result != '-').any(axis=0)]
        
        result = CrossTabs(merge_result)
        result.attrs['type'] = list(set(qtypes))
        return result


    def grid_summary(self, index: Union[List[str], List[List[str]], Tuple[str], CrossTabs],
                    summary_name: str = '',
                    cond: Optional[pd.Series] = None,
                    base_desc: Optional[str] = None,
                    **kwargs) :
        if not isinstance(index, list):
            raise ValueError(f'index must be list : {index}')

        summary_df = []
        cond = (self.attrs['default_filter']) if cond is None else (self.attrs['default_filter']) & (cond)
        df = self[cond].copy()

        
        for idx in index :
            table = None
            if isinstance(idx, CrossTabs):
                table = idx
            else : 
                table = self.table(idx, cond=cond, **kwargs)

            index_name = idx
            col_name = idx
            if isinstance(idx, tuple) :
                idx = self.ma_return(idx)
                if not self.col_name_check(*idx) : return

            if isinstance(idx, list) :
                index_name = idx[0]
            
            sub_title = None
            titles = self.attrs['title']
            if titles :
                if index_name in titles.keys() :
                    sub_title = titles[index_name]['sub_title']
                    if sub_title is None :
                        sub_title = titles[index_name]['title']
            
            if isinstance(idx, list) :
                if len(idx) == 1 :
                    index_name = idx[0]
                else :
                    index_name = f'{idx[0]}-{idx[-1]}'

            col_name = index_name if sub_title is None else f'[{index_name}] {sub_title}'
            table = table.rename(columns={'Total': col_name})
            summary_df.append(table)
        
        qtypes = [x.attrs['type'] for x in summary_df]
        qtypes = list(set(qtypes))
        summary = pd.concat(summary_df, axis=1)
        
        # summary.index.name = summary_name
        if isinstance(summary.index, pd.MultiIndex) :
            summary.index = summary.index.droplevel(0)
        
        summary.index = pd.MultiIndex.from_tuples([(summary_name, idx) for idx in summary.index])
        result = CrossTabs(summary)
        result.attrs['type'] = qtypes
        result.columns = pd.MultiIndex.from_tuples([(f'Variable : {len(index)}', idx) for idx in summary.columns])
        

        if base_desc is None :
            sample_count = len(self)
            all_count = [x for x in list(result.iloc[0, :])]
            dup_chk = list(set(all_count))
            if len(dup_chk) > 1 :
                base_desc = 'Difference Total'
            else :
                all_count = all_count[0]
                if sample_count == all_count :
                    base_desc = 'All Base'
                else :
                    sample_ratio = round(all_count/sample_count, 2) * 100
                    base_desc = f'Not All Base ({sample_ratio:.0f}%)'

        var_names = [f'{i[0]}-{i[-1]}' if isinstance(i, list) else i for i in index]
        var_names = '/'.join(var_names)


        result.index.names = pd.Index([var_names, base_desc])
        return result

    def get_title(self, qid: str) :
        title = self.attrs['title']
        if title :
            if qid in title.keys():
                qtitle = title[qid]['title']
                sub_title = title[qid]['sub_title']
                if sub_title :
                    return f'{qtitle}_{sub_title}'
                else :
                    return qtitle
            else :
                return qid
        else :
            return qid


    def proc_append(self, 
                    table_id: Union[str, tuple], 
                    table: Union[pd.DataFrame, CrossTabs],
                    ai: bool = False,
                    model: Literal['gpt-4o', 'gpt-4o-mini', 'llama3', 'llama3.1'] = 'gpt-4o-mini',
                    prompt: Optional[str] = None,
                    heatmap: Optional[bool] = True,) :
        if not isinstance(table_id, (str, tuple)) :
            raise ValueError(f'table_id must be str or tuple')
        
        table_name = table_id
        table_desc = None

        if isinstance(table_id, tuple) :
            if len(table_id) != 2 :
                raise ValueError(f'table_id must be tuple with 2 elements')
            else :
                table_name = table_id[0]
                table_desc = table_id[1]
                

        if not isinstance(table, (pd.DataFrame, CrossTabs)) :
            raise ValueError(f'table must be pd.DataFrame or CrossTabs')
        
        if not isinstance(table, CrossTabs) :
            table = CrossTabs(table)
        
        proc_result = self.attrs['proc_result']
        
        if table_name in proc_result.keys() :
            print(f'⚠️ result title already exists : {table_name}')
        
        chat_result = None
        
        table_type = table.attrs['type']
        if ai :
            chat_result = table.chat_ai(model=model, 
                                        prompt=prompt, 
                                        with_table=False, 
                                        table_type=table_type,
                                        sub_title=table_desc)

        proc_result[table_name] = {
            'desc': table_desc, 
            'table': table,
            'ai': chat_result
        }

        table_html = None
        if table_type in ['number', 'text'] :
            table_html = table.to_html(escape=False, index=True, border=0, classes='table table-striped table-hover')
        else :
            table_html = table.ratio(heatmap=heatmap).to_html()

        table_desc_html = f"""<div style="font-size: 0.8rem; padding: 7px; max-width: 600px; font-style: italic; margin-bottom: 7px;">
                {table_desc}
        </div>"""
        
        table_analysis_html = f"""<div style="font-weight: bold; font-size: 0.8rem; padding: 7px; max-width: 700px; margin-bottom: 7px;border: 1px solid #2d6df6; border-radius: 5px;">
                {chat_result}
        </div>
        """

        table_id_html = f"""
            <div style="width:fit-content;padding: 7px; font-size:1rem;font-weight:bold; background-color: #2d6df6; border-radius: 5px; color:white; margin-bottom: 7px;">
                {table_name}
            </div>
            {table_desc_html if table_desc is not None else ''}
            {table_analysis_html if chat_result is not None else ''}
            <div>
                {table_html}
            </div>
        """
        display(HTML(table_id_html))
    
    
    def proc_export_excel(self, file_name: str, heatmap: bool = False) :
        total_label = 'Total'
        proc_result = self.attrs['proc_result']
        if not proc_result : 
            raise ValueError('No result to export')
        
        excel.ExcelFormatter.header_style = {
            "font": {"bold": True, "size": 9},
            "borders": {
                "top": "medium",
                "right": "thin",
                "bottom": "medium",
                "left": "thin",
            },
            "alignment": {"horizontal": "center", "vertical": "center", "wrapText": True},
            "fill": {"bgColor": "#DCE6F1"}
        }

        # 엑셀 파일 생성
        file_name = f'{file_name}.xlsx'
        writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
        workbook = writer.book

        # 목차 시트 추가
        index_sheet = workbook.add_worksheet('Index')
        index_format = workbook.add_format({
            'align': 'center', 
            'bold': True, 
            'underline': True, 
            'font_size': 11,
            'font_color': '#2d6df6', 
            'border': 1})
        
        desc_format = workbook.add_format({
            'align': 'left', 
            'italic': True,
            'font_size': 11,
            'border': 1})

        qid_format = workbook.add_format({
            'align': 'center', 
            'font_size': 11,
            'border': 1})

        head_format = workbook.add_format({
            'align': 'center', 
            'font_size': 12,
            'bold': True, 
            'border': 1, 
            'bg_color': '#DDEBF7'})

        table_head = workbook.add_format({
            'align': 'center',
            'font_size': 9,
            'bold': True,
            'border': 1,
            'top': 2,
            'bottom': 2,
            'bg_color': '#DCE6F1',
        })
        

        # 데이터프레임을 저장할 시트 추가
        data_sheet = workbook.add_worksheet('Table')

        # B열 틀고정
        data_sheet.freeze_panes(0, 2)

        # 목차 시트에 하이퍼링크 추가
        row = 1
        col = 0
        data_start_row = 2


        index_sheet.write(0, 0, 'Table Index', head_format)
        index_sheet.write(0, 1, 'Table Description', head_format)
        index_sheet.write(0, 2, 'Variable', head_format)
        index_sheet.write(0, 3, 'Base', head_format)

        # 너비 설정
        index_sheet.set_column('A:A', 20) # Talbe Index
        index_sheet.set_column('B:B', 60) # Table Description
        index_sheet.set_column('C:C', 20) # Variable
        index_sheet.set_column('D:D', 30) # Base

        data_sheet.set_column('A:A', 15)
        data_sheet.set_column('B:B', 30)
        data_sheet.set_column('C:C', 7)

        for key, table_attrs in proc_result.items():
            result = table_attrs['table']
            desc = table_attrs['desc']
            ai = table_attrs['ai']

            new_group_name = {
                'index': '',
                'columns': key
            }
            for item, gr_name in new_group_name.items() :
                base = getattr(result, item)
                if not isinstance(base, pd.MultiIndex) :
                    setattr(result, item, pd.MultiIndex.from_tuples([('' if b == total_label else gr_name, b) for b in base]))

                else :
                    group_lenth = base.nlevels
                    if group_lenth > 2 :
                        setattr(result, item, pd.MultiIndex.from_tuples([('' if b[-1]==total_label else b[-2], b[-1]) for b in base]))

            index_header = None
            if all(i is None for i in result.index.names) :
                if total_label in result.index.get_level_values(-1) and total_label in result.columns.get_level_values(-1) :
                    all_count = result.loc[('', total_label), ('', total_label)]
                    sample_count = len(self)
                    if sample_count == all_count :
                        index_header = 'All Base'
                    else :
                        sample_ratio = round(all_count/sample_count, 2) * 100
                        index_header = f'Not All Base ({sample_ratio:.0f}%)'
                    
                    result.index.names = pd.Index(['', index_header])

            index_sheet.write_url(row, col, f'internal:Table!A{data_start_row+1}', string=key, cell_format=index_format)
            index_sheet.write(row, col + 1, desc, desc_format)

            base_desc = None
            qid_name = None

            if isinstance(result, CrossTabs) :
                qid_name = result.index.names[0]
                base_desc = result.index.names[-1]
                
                resurt_type = result.attrs['type']
                
                if isinstance(resurt_type, list) :
                    if all(not x in ['number'] for x in resurt_type) :
                        result = result.ratio(heatmap=False)
                    
                elif not resurt_type in ['number'] :
                    result = result.ratio(heatmap=False)
                

            index_sheet.write(row, col + 2, qid_name, qid_format)
            index_sheet.write(row, col + 3, base_desc, qid_format)

            row += 1

            result.to_excel(writer, 
                            sheet_name='Table', 
                            startrow=data_start_row, 
                            startcol=0, engine='openpyxl')

            data_sheet.merge_range(data_start_row, col, data_start_row, col+1, key, table_head)
            data_sheet.merge_range(data_start_row+1, col, data_start_row+1, col+1, desc, table_head)
            
            
            zero_float_format = workbook.add_format({
                'num_format': '0',
                'align': 'center',
                'border': 1,
                'font_size': 9,
            })

            float_format = workbook.add_format({
                'num_format': '0.00',
                'align': 'center',
                'border': 1,
                'font_size': 9,
            })

            default_format = workbook.add_format({
                'align': 'center',
                'border': 1,
                'font_size': 9,
            })
            
            total_format = workbook.add_format({
                'num_format': '0',
                'align': 'center',
                'border': 1,
                'font_size': 9,
                'bold': True,
            })

            head_row = data_start_row + 1
            blank_row = data_start_row + 2
            format_start = data_start_row+3



            set_group = []
            for col_idx, col_name in enumerate(result.columns) :
                set_col = col_idx+2
                group_col_name = col_name[0]
                set_col_name = col_name[-1]
                if col_idx == 0 and set_col_name == 'Total' :
                    data_sheet.write(data_start_row, set_col, None, table_head)
                    data_sheet.write(head_row, set_col, None, table_head)
                    data_sheet.write(blank_row, set_col, set_col_name, table_head)
                else :
                    data_sheet.write(blank_row, set_col, set_col_name, table_head)
                    
                    if not group_col_name in set_group :
                        if group_col_name != '' and group_col_name is not None :
                            set_group.append(group_col_name)
                            merge_col_count = len([x for x in result.columns if x[0] == group_col_name]) - 1
                            end_col = set_col+merge_col_count
                            for m in range(set_col, end_col+1) :
                                data_sheet.write(data_start_row, m, f'#{len(set_group)}', table_head)
                            
                            if merge_col_count > 0 :
                                data_sheet.merge_range(head_row, set_col, head_row, set_col+merge_col_count, group_col_name, table_head)
                            else :
                                data_sheet.write(head_row, set_col, group_col_name, table_head)
            
            if len(set_group) == 1 :
                data_sheet.write(data_start_row, 2, None, table_head)

            last_row = None
            for df_row, i in enumerate(range(format_start, format_start+len(result))) :
                for df_col, j in enumerate(range(2, len(result.columns)+2)) :
                    cell_value = result.iloc[df_row, df_col]
                    if pd.isna(cell_value) :
                        cell_value = '-'

                    df_row_name = result.index[df_row]

                    if isinstance(result.index, pd.MultiIndex) :
                        df_row_name = result.index[df_row][-1]

                    if df_row_name == 'Total' :
                        data_sheet.write(i, j, cell_value, total_format)
                    else :
                        data_sheet.write(i, j, cell_value, zero_float_format)
                        if heatmap :
                            if cell_value != '-' :
                                bg_color = calculate_bg_color(float(cell_value))
                                data_sheet.write(i, j, cell_value, workbook.add_format({
                                    'num_format': '0',
                                    'align': 'center',
                                    'border': 1,
                                    'font_size': 9,
                                    'bg_color': bg_color
                                }))
                        if df_row_name in ['mean', 'min', 'max', 'std'] :
                            data_sheet.write(i, j, cell_value, float_format)

            last_row = i + 1

            if ai is not None :
                ai_result_format = workbook.add_format({
                    'font_size': 9,
                    'text_wrap': True,  # 자동 줄바꿈 설정
                    'valign': 'top',
                    'align': 'left',
                })
                data_sheet.merge_range(last_row, col+2, last_row, col+12, ai, ai_result_format)
                data_sheet.set_row(last_row, 150)

            data_start_row += len(result) + 6  # 3행 간격

            
        writer.close()
        
        wb = load_workbook(file_name)
        ws = wb['Table']
        
        # 열 B의 서식 설정: 오른쪽 정렬
        for cell in ws['B']:
            cell.alignment = Alignment(horizontal='right')

        # Last Column


        wb.save(file_name)


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


class DefaultArgs(TypedDict, total=False):
    top: Union[int, List[int]]
    medium: Union[bool, int, List[int]]
    bottom: Union[int, List[int]]
    with_value: bool


def DataProcessing(dataframe: pd.DataFrame, 
                   keyid: Optional[str] = None,
                   default_args: Optional[DefaultArgs] = {
                    'top': 2,
                    'medium': True,
                    'bottom': 2,
                    'with_value': False
                    }):
    module_path = os.path.dirname(__file__)
    css_path = os.path.join(os.path.dirname(module_path), 'dataCheck')
    css = get_css(os.path.join(css_path, 'styles.css'))
    display(HTML(css))
    df = convert_columns_to_nullable_int(dataframe)

    default_args = {f'default_{key}': value for key, value in default_args.items()}

    return DataCheck(df, 
                     css=css, 
                     keyid=keyid,
                     **default_args)


def DecipherDataProcessing(dataframe: pd.DataFrame, 
                           keyid: Optional[str] = "record",
                           map_json: Optional[str] = None,
                           meta_path: Optional[str] = None,
                           title_path: Optional[str] = None,
                           default_args: Optional[DefaultArgs] = {
                               'top': 2,
                               'medium': True,
                               'bottom': 2,
                               'with_value': False
                           }) :
    module_path = os.path.dirname(__file__)
    css_path = os.path.join(os.path.dirname(module_path), 'dataCheck')
    css = get_css(os.path.join(css_path, 'styles.css'))
    display(HTML(css))
    df = convert_columns_to_nullable_int(dataframe)

    metadata = None
    title = None

    if map_json is None :
        if meta_path is not None:
            try:
                with open(meta_path, 'r', encoding='utf-8') as meta_file:
                    metadata = json.load(meta_file)
            except FileNotFoundError:
                print(f"File not found: {meta_path}")

        if title_path is not None:
            try:
                with open(title_path, 'r', encoding='utf-8') as title_file:
                    title = json.load(title_file)
            except FileNotFoundError:
                print(f"File not found: {title_path}")
    else :
        try :
            _map = None
            with open(map_json, 'r', encoding='utf-8') as map_file:
                    _map = json.load(map_file)
            metadata = {}
            title = {}

            for m in _map :
                base = m['variables']
                variables = [list(v.keys())[0] for v in base]
                qtype = m['type']
                meta = m['meta']
                grouping = m['grouping']
                mtitle = m['title']

                title[m['qlabel']] = {
                    "type": qtype,
                    "title": mtitle,
                    "sub_title": None,
                    "vgroup": None
                }

                metadata[m['qlabel']] = meta

                for v in variables :
                    qtitle = m['title']
                    base_var = [b[v] for b in base if list(b.keys())[0] == v][0]
                    
                    if qtype in ['single', 'rating', 'rank'] :
                        metadata[v] = meta
                    elif qtype in ['other_open'] :
                        metadata[v] = list(meta[0].values())[0]
                    else :
                        if grouping == 'rows' :
                            metadata[v] = [list(i.values())[0]['colTitle'] for i in meta if list(i.keys())[0] == v][0]
                        else :
                            metadata[v] = [list(i.values())[0]['rowTitle'] for i in meta if list(i.keys())[0] == v][0]
                    
                    sub_title = None
                    if grouping == 'rows' :
                        sub_title = base_var['rowTitle']
                        if sub_title is None :
                            sub_title = base_var['colTitle']
                    if grouping == 'cols' :
                        sub_title = base_var['colTitle']
                        if sub_title is None :
                            sub_title = base_var['rowTitle']

                    title[v] = {
                        "type": qtype,
                        "title": qtitle,
                        "sub_title": sub_title,
                        "vgroup": base_var['vgroup']
                    }

        except FileNotFoundError :
            print(f"File not found: {title_path}")
    

    default_args = {f'default_{key}': value for key, value in default_args.items()}

    return DataCheck(df, 
                     css=css, 
                     keyid=keyid,
                     meta=metadata, 
                     title=title, 
                     **default_args)
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
            cond: Optional[str] = None,
            use_variable: bool = False,
            key: str = api_key, 
            server: str = api_server, 
            meta: bool = True, 
            data_layout: bool = False, 
            base_layout: str = 'DoNotDelete',
            mkdir: bool = False,
            dir_name: Optional[str] = None) -> None:

    """
    데이터 체크 노트북 파일 및 데이터 세팅
    
    Args:
        `pid` (str): 프로젝트 ID.
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

    parent_path = os.getcwd()
    if mkdir :
        folder_name = pid
        if dir_name != None :
            folder_name = dir_name
        parent_path =  os.path.join(parent_path, folder_name)
        chk_mkdir(parent_path)


    # META DATA
    map_py = decipher_map(pid) # import variable
    if meta :
        meta_path = os.path.join(parent_path, 'meta')
        ensure_directory_exists(meta_path)
        metadata = decipher_meta(pid) # attr meta
        title = decipher_title(pid) # title meta

        with open(os.path.join(meta_path, f'meta_{pid}.json'), 'w', encoding='utf-8') as f :
            json.dump(metadata, f, ensure_ascii=False, indent=4)
        
        with open(os.path.join(meta_path, f'title_{pid}.json'), 'w', encoding='utf-8') as f :
            json.dump(title, f, ensure_ascii=False, indent=4)

        with open(os.path.join(meta_path, f'map_{pid}.json'), 'w', encoding='utf-8') as f :
            json.dump(map_py, f, ensure_ascii=False, indent=4)

        with open(os.path.join(meta_path, f'variables_{pid}.py'), 'w', encoding='utf-8') as f :
            for mp in map_py :
                qlabel = mp['qlabel']
                variables = mp['variables']
                variables = [list(v.keys())[0] for v in variables]
                qtype = mp['type']
                var_text = f"""# {qlabel} : {qtype}\n"""

                if len(variables) >= 2 :
                    for v in variables :
                        var_text += f"""{v} = '{v}'\n"""
                
                if len(variables) == 1 :
                    main_qlabel = variables[0]
                    qlabel = main_qlabel
                    variables = f"""'{main_qlabel}'"""
                
                values = mp['values'] if 'values' in mp.keys() else None
                attrs = mp['attrs'] if 'attrs' in mp.keys() else None

                var_text += f"""{qlabel} = {variables}\n"""
                if values :
                    var_text += f"""{qlabel}_value = {values}\n"""

                if attrs :
                    var_text += f"""{qlabel}_attrs = {attrs}\n"""
                var_text += "\n"
                f.write(var_text)
    #----

    # DATA DOWNLOAD
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

    csv_data = get_decipher_data(pid, data_format='csv', cond=delivery_cond)
    sav_data = get_decipher_data(pid, data_format='spss16', cond=delivery_cond)

    csv_binary = f'binary_{pid}.csv'
    data_path = os.path.join(parent_path, 'data')
    ensure_directory_exists(data_path)
    create_binary_file(data_path, csv_binary, csv_data)
    create_ascii_file(data_path, csv_binary, f'{pid}.csv')
    
    sav_zip = f'{pid}_sav.zip'
    create_binary_file(data_path, sav_zip, sav_data)
    unzip_and_delete(os.path.join(data_path, sav_zip), data_path)
    #----

    # DATA CHECK SETTING
    map_xlsx = get_decipher_datamap(pid, 'xlsx')
    create_binary_file(data_path, f'mapsheet_{pid}.xlsx', map_xlsx)

    xl = openpyxl.load_workbook(os.path.join(data_path, f'mapsheet_{pid}.xlsx'))
    map_sheet = 'datamap'
    data_map = xl[map_sheet]

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
        c_cell = variable['c']
        qid = a_cell[0] # qid

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

    # DATACHECK NOTEBOOK
    nb = nbf.v4.new_notebook()
    ipynb_file_name = f'DataCheck_{pid}.ipynb'
    order_qid = list(qids.items())

    ipynb_cell = []

    # set_file_name = 'pd.read_excel(file_name)' if mode == 'file' else 'pd.read_csv(file_name, low_memory=False)'

    excel_meta = f'''DecipherDataProcessing(df, map_json=f"meta/map_{{pid}}.json")''' if meta else '''DecipherDataProcessing(df)'''

    default = f'''import pandas as pd
import pyreadstat
import numpy as np
from meta.variables_{pid} import * 
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

                    for qel in qels :
                        if na in qel :
                            cell_texts.append(f'# The {qid} contains {qel}')
                        else :
                            safreq = f"df.safreq('{qel}')"
                            if use_variable : safreq = f"df.safreq({qel})"

                            cell_texts.append(safreq)

                    if val_label :
                        values = [v for v in val_label.keys() if not int(v) == 0]

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

                            dupchk = f"df.dupchk({set_qid})"
                            if use_variable : dupchk = f"df.dupchk({qid})"

                            cell_texts.append(dupchk)
                else :
                    if qval :
                        val_chk = f"# value : {qval}"
                        
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


                    if val_label :
                        values = [v for v in val_label.keys() if not int(v) == 0]

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

                        cell_texts.append(safreq)

            ### num end ###

            # text check #
            elif qtype == 'OE' :
                if len(qels) >=2 :
                    diff_na = [q for q in qels if not na in q]

                for qel in qels :
                    if na in qel :
                        cell_texts.append(f'# The {qid} contains {qel}')
                    else :
                        safreq = f"df.safreq('{qel}')"
                        if use_variable : safreq = f"df.safreq({qel})"

                        cell_texts.append(safreq)
            ### text end ###

            # other open check #
            elif qtype == 'OTHER_OE' :
                for qel in qels :
                    safreq = f"df.safreq('{qel}')"
                    if use_variable : safreq = f"df.safreq({qel})"

                    cell_texts.append(safreq)
            ### other open end ###


            if cell_texts :
                cell = '\n'.join(cell_texts)
                ipynb_cell.append(nbf.v4.new_code_cell(cell))
            else :
                mark = f'The {qid} not cotains elements'
                ipynb_cell.append(nbf.v4.new_markdown_cell(mark))

    #ipynb_cell
    nb['cells'] = ipynb_cell
    #print(nb)
    ipynb_file_path = os.path.join(parent_path, ipynb_file_name)
    if not os.path.isfile(ipynb_file_path) :
        with open(ipynb_file_path, 'w') as f:
            nbf.write(nb, f)
    else :
        print('❗ The DataCheck ipynb file already exists')

    #----

    # LAYOUT
    if data_layout :
        layouts = decipher_create_layout(pid, base_layout=base_layout, qids=qids)
        ce_layout = layouts['CE']
        oe_layout = layouts['OE']
        
        layout_path = os.path.join(parent_path, 'layout')
        ensure_directory_exists(layout_path)
        with open(os.path.join(layout_path, f'Close_Ended_{pid}.txt'), 'w', encoding='utf-8') as f :
            f.write(ce_layout)

        with open(os.path.join(layout_path, f'Open_Ended_{pid}.txt'), 'w', encoding='utf-8') as f :
            f.write(oe_layout)
    #----

    #---    
    print("✅ Setting Complete")