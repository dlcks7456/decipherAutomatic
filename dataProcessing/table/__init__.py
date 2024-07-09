import pandas as pd
from IPython.display import display, HTML
from typing import Union, List, Tuple, Dict, Optional, Literal, Callable, Any, NoReturn, Type
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
from pandas.io.formats import excel
import zipfile

def custom_calc(df: pd.DataFrame, 
                index: str, 
                columns: Union[str, List[str]],
                total_label: str = 'Total',
                include_total: bool = True,
                aggfunc: Union[str, List[str]] = ['mean'], 
                float_round: int = 2) -> pd.DataFrame:
    """
    Calculates descriptive statistics for the specified index column based on the values of the columns parameter.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    index (str): The column name to group by and calculate statistics for.
    columns (Union[str, List[str]]): The column name(s) to use for grouping.
    aggfunc (Union[str, List[str]]): The aggregation function(s) to apply. Default is 'mean'.
    float_round (int): Number of decimal places to round the results to. Default is 2.

    Returns:
    pd.DataFrame: A DataFrame containing the calculated statistics.
    """
    
    # Validate index parameter
    if not isinstance(index, str):
        raise ValueError("Index parameter must be a string representing a column name.")
    
    # Ensure aggfunc is a list
    if isinstance(aggfunc, str):
        aggfunc = [aggfunc]

    # Initialize an empty DataFrame for the results
    ndf = pd.DataFrame()
    
    # Check if columns is a string or a list
    def set_value(value) :
        if value is None :
            return np.nan
        else :
            return str(value)

    if isinstance(columns, str):
        # Single column case
        values = df[columns].value_counts().index.to_list()
        if include_total :
            desc = df[df[columns].notna()][index].describe().round(float_round).to_dict()
            for af in aggfunc:
                ndf.loc[af, total_label] = set_value(desc[af])

        for v in values:
            desc = df[df[columns] == v][index].describe().round(float_round).to_dict()
            for af in aggfunc:
                ndf.loc[af, str(v)] = set_value(desc[af])
        
    elif isinstance(columns, list):
        if include_total :
            desc = df[(df[columns]!=0).any(axis=1) & (~df[columns].isna()).any(axis=1)][index].describe().round(float_round).to_dict()
            for af in aggfunc:
                ndf.loc[af, total_label] = set_value(desc[af])

        # Binary data case
        for col in columns:
            desc = df[(~df[col].isna()) & (df[col] != 0)][index].describe().round(float_round).to_dict()
            for af in aggfunc:
                ndf.loc[af, col] = set_value(desc[af])

    else:
        raise ValueError("Columns parameter must be either a string or a list of column names.")

    return ndf


def create_crosstab(df: pd.DataFrame,
                    index: Union[str, List[str]],
                    columns: Optional[Union[str, List[str]]] = None,
                    index_meta: Optional[List[Dict[str, str]]] = None,
                    columns_meta: Optional[List[Dict[str, str]]] = None,
                    include_total: bool = False,
                    index_name: Optional[Union[str, bool]] = None,
                    columns_name: Optional[Union[str, bool]] = None,
                    fill: bool = True,
                    top: Optional[Union[int, List[int]]] = None,
                    medium: Optional[Union[int, List[int], bool]] = True,
                    bottom: Optional[Union[int, List[int]]] = None,
                    aggfunc: Optional[list] = None,
                    float_round: int = 2,
                    sort_index: Optional[str] = None) -> pd.DataFrame:
    """
    Creates a crosstab from the provided DataFrame with optional metadata for reordering and relabeling indices and columns, and with options to include top/bottom summaries and index sorting.
    
    Parameters:
        df (pd.DataFrame): The input DataFrame.
        index (str or list): The column name or list of column names to use for the crosstab index.
        columns (str or list, optional): The column name or list of column names to use for the crosstab columns.
        index_meta (list of dict, optional): Metadata for the index values and labels.
        columns_meta (list of dict, optional): Metadata for the columns values and labels.
        include_total (bool, optional): Whether to include the total sum row.
        index_name (str, optional): The name to assign to the crosstab index.
        columns_name (str, optional): The name to assign to the crosstab columns.
        top (int, optional): Number of top rows to summarize.
        bottom (int, optional): Number of bottom rows to summarize.
        sort_index (str, optional): How to sort the index. 'asc' for ascending, 'desc' for descending, None for no sorting.
    
    Returns:
        pd.DataFrame: The resulting crosstab with optional reordering, relabeling, top/bottom summaries, and total sum row.
    """
    
    total_label = 'Total'
    count_label = 'Count'

    def extract_order_and_labels(metadata):
        """
        Extracts the order and labels from the provided metadata.
        
        Parameters:
            metadata (list of dict): The metadata to extract order and labels from.
        
        Returns:
            order (list): The extracted order of keys.
            labels (list): The extracted labels for the keys.
        """
        order = [list(d.keys())[0] for d in metadata]
        labels = [list(d.values())[0] for d in metadata]
        return order, labels
    
    def add_missing_indices(df, order):
        """
        Adds missing indices with zero values to the DataFrame.
        
        Parameters:
            df (pd.DataFrame): The DataFrame to add missing indices to.
            order (list): The list of indices to ensure exist in the DataFrame.
        
        Returns:
            pd.DataFrame: The DataFrame with missing indices added.
        """
        
        for idx in order:
            if idx not in df.index :
                df.loc[idx] = 0
        return df

    def reorder_and_relabel(df, order, labels, axis, name):
        """
        Reorders and relabels the DataFrame based on the provided order and labels.
        
        Parameters:
            df (pd.DataFrame): The DataFrame to reorder and relabel.
            order (list): The order to apply.
            labels (list): The labels to apply.
            axis (int): The axis to apply the reorder and relabel (0 for index, 1 for columns).
            name (str): The name to assign to the index or columns.
        
        Returns:
            pd.DataFrame: The DataFrame with reordered and relabeled indices/columns.
        """
        if axis == 0:
            df = df.loc[order]
            if name is not None and name is not False :
                df.index = pd.Index(labels, name=name)
            else :
                df.index = pd.Index(labels)
        else:
            df = df[order]
            if name is not None and name is not False :
                df.columns = pd.Index(labels, name=name)
            else :
                df.columns = pd.Index(labels)
        return df

    def create_binary_crosstab(df, index_cols, columns_col=None, include_total=False):
        """
        Creates a crosstab for binary columns in the provided DataFrame.

        Parameters:
            df (pd.DataFrame): The input DataFrame.
            index_cols (list): The list of column names to use for the crosstab index.
            columns_col (str or list, optional): The column name or list of column names to use for the crosstab columns.
            include_total (bool, optional): Whether to include totals for rows and columns.

        Returns:
            pd.DataFrame: The resulting binary crosstab.
        """
        def count_binary_values(idx, col):
            return (((df[idx] != 0) & (df[col] != 0) & df[idx].notna() & df[col].notna())).sum()

        def count_values_mixed(sa, ma_index, ma_cols) :
            return ((df[sa] != 0) & (df[ma_cols] == ma_index) & df[sa].notna()).sum()
    
        def ma_total(ma, sa) :
            return ((df[ma]!=0).any(axis=1) & (~df[ma].isna()).any(axis=1) & (~df[sa].isna())).sum()

        def ma_sa_count(ma, sa) :
            return ((df[ma] != 0) & (df[ma].notna()) & (~df[sa].isna())).sum()
    
        def sa_ma_count(sa, ma) :
            return ((df[sa]==col) & (df[ma]!=0).any(axis=1) & (~df[ma].isna()).any(axis=1)).sum()

        def ma_ma_count(ma_cols, ma_var) :
            return ((df[ma_cols]!=0).any(axis=1) & (~df[ma_cols].isna()).any(axis=1) & (df[ma_var]!=0) & (~df[ma_var].isna())).sum()
    
        def ma_ma_total(index_ma, column_ma) :
            return ((df[index_ma]!=0).any(axis=1) & (~df[index_ma].isna()).any(axis=1) & (df[column_ma]!=0).any(axis=1) & (~df[column_ma].isna()).any(axis=1)).sum()

        if columns_col is None:
            # Create a crosstab with a single "Count" column if no columns_col is provided
            crosstab_result = pd.DataFrame(index=index_cols, columns=[count_label])
            for idx in index_cols:
                count = count_binary_values(idx, idx)
                crosstab_result.loc[idx, count_label] = count
        else:
            if isinstance(index_cols, str) and isinstance(columns_col, list) :
                # Extract unique values from the single column
                unique_cols = df[columns_col].dropna().unique()
                # Create a crosstab with the unique columns
                crosstab_result = pd.DataFrame(index=index_cols, columns=unique_cols)
                for idx in index_cols:
                    for col in unique_cols:
                        count = count_values_mixed(col, idx, index_cols)
                        crosstab_result.loc[idx, col] = count

                        if include_total:
                            crosstab_result.loc[col, total_label] = ma_sa_count(col, index_cols)
                            
                    if include_total:
                        crosstab_result.loc[total_label, idx] = sa_ma_count(columns_col, index_cols)

                if include_total :
                    crosstab_result.loc[total_label, total_label] = ma_total(columns_col, index_cols)

            if isinstance(index_cols, list) and isinstance(columns_col, str) :
                # Extract unique values from the single column
                unique_cols = df[columns_col].dropna().unique()
                # Create a crosstab with the unique columns
                crosstab_result = pd.DataFrame(index=index_cols, columns=unique_cols)
                for idx in index_cols:
                    for col in unique_cols:
                        count = count_values_mixed(idx, col, columns_col)
                        crosstab_result.loc[idx, col] = count

                        if include_total:
                            crosstab_result.loc[total_label, col] = sa_ma_count(columns_col, index_cols)
                            
                    if include_total:
                        crosstab_result.loc[idx, total_label] = ma_sa_count(idx, columns_col)
                    
                if include_total :
                    crosstab_result.loc[total_label, total_label] = ma_total(index_cols, columns_col)
                
            elif isinstance(index_cols, list) and isinstance(columns_col, list):
                # Create a DataFrame to hold the crosstab result
                crosstab_result = pd.DataFrame(index=index_cols, columns=columns_col)
                for idx in index_cols:
                    for col in columns_col:
                        count = count_binary_values(idx, col) if idx != col else count_binary_values(idx, idx)
                        crosstab_result.loc[idx, col] = count
                        if include_total :
                            crosstab_result.loc[total_label, col] = ma_ma_count(index_cols, col)
                    
                    if include_total :
                        crosstab_result.loc[idx, total_label] = ma_ma_count(columns_col, idx)
                
                if include_total :
                    crosstab_result.loc[total_label, total_label] = ma_ma_total(index_cols, columns_col)
            else:
                raise ValueError("columns_col must be either a string or a list of strings.")
        
        if include_total :
            crosstab_result[total_label] = crosstab_result[total_label].astype(int)

        return crosstab_result


    # Determine if we are working with single or multiple columns for index
    if isinstance(index, str):
        index_is_binary = False
    elif isinstance(index, list):
        index_is_binary = True
    else:
        raise ValueError("Index must be either a string or a list of strings.")
    
    # Create the appropriate crosstab
    if columns is None:
        if index_is_binary:
            # Create frequency table for binary columns
            crosstab_result = pd.DataFrame(index=index, columns=[count_label])
            for idx in index:
                crosstab_result.loc[idx, count_label] = (df[idx] != 0).sum()
            
            if include_total :
                crosstab_result.loc[total_label] = ((df[index]!=0).any(axis=1) & (~df[index].isna()).any(axis=1)).sum()
        else:
            crosstab_result = df[index].value_counts().to_frame(name=count_label)
            if include_total :
                crosstab_result.loc[total_label] = crosstab_result[count_label].sum()
    else:
        if isinstance(columns, str):
            columns_is_binary = False
        elif isinstance(columns, list):
            columns_is_binary = True
        else:
            raise ValueError("Columns must be either a string or a list of strings.")
        
        if index_is_binary and columns_is_binary:
            crosstab_result = create_binary_crosstab(df, index, columns, include_total)
        elif index_is_binary:
            crosstab_result = create_binary_crosstab(df, index, columns, include_total)
        elif columns_is_binary:
            crosstab_result = create_binary_crosstab(df, columns, index, include_total).T
        else:
            crosstab_result = pd.crosstab(
                index=df[index],
                columns=df[columns],
                margins=include_total, 
                margins_name=total_label
            )

    crosstab_result.index = crosstab_result.index.map(str)
    crosstab_result.columns = crosstab_result.columns.map(str)

    calc = None
    if aggfunc is not None :
        calc = custom_calc(df, index=index, columns=columns, aggfunc=aggfunc, float_round=float_round)

    # Process index metadata
    if index_meta:
        index_order, index_labels = extract_order_and_labels(index_meta)
        crosstab_result = add_missing_indices(crosstab_result, index_order)

        total_row = None
        if include_total :
            total_row = crosstab_result.loc[total_label, :]

        crosstab_result = reorder_and_relabel(crosstab_result, index_order, index_labels, axis=0, name=index_name)
        
        if total_row is not None :
            crosstab_result.loc[total_label] = total_row

    # Process columns metadata
    if columns_meta:
        columns_order, columns_labels = extract_order_and_labels(columns_meta)
        crosstab_result = add_missing_indices(crosstab_result.T, columns_order).T

        total_col = None
        if include_total :
            total_col = crosstab_result.loc[:, total_label]
        
        crosstab_result = reorder_and_relabel(crosstab_result, columns_order, columns_labels, axis=1, name=columns_name)

        if total_col is not None :
            crosstab_result.loc[:, total_label] = total_col

        # Calc Resulrt DataFrame
        if calc is not None :
            calc = add_missing_indices(calc.T, columns_order).T
            
            calc_col = None
            if include_total :
                calc_col = calc.loc[:, total_label]
            
            calc = reorder_and_relabel(calc, columns_order, columns_labels, axis=1, name='desciription')
            
            if calc_col is not None :
                calc.loc[:, total_label] = calc_col
            
    
    # Sort index if sort_index is specified
    original_index_order = crosstab_result.index.to_list()


    medium_auto_flag = False
    if all([n is not None for n in [top, bottom]]) :
        sort_index = 'desc'
        medium_auto_flag = True
        
    if sort_index is not None:
        ascending = True if sort_index == 'asc' else False
        
        # Exclude 'Total' from sorting temporarily
        if include_total :
            total_row = crosstab_result.loc[total_label]
            crosstab_result = crosstab_result.drop(total_label)

        crosstab_result = crosstab_result.sort_index(ascending=ascending)

        # Add total_label back to the beginning of the index
        if include_total :
            crosstab_result = pd.concat([pd.DataFrame([total_row]), crosstab_result])

        original_index_order = crosstab_result.index.to_list()
    

    # Add top and bottom summaries if needed
    top_cols = []
    if top is not None:
        top_list = top
        if isinstance(top, int) :
            top_list = [top]
        top_list = list(set(top_list))
        top_list.sort(reverse=True)
        
        top_result = []
        for t in top_list :
            top_indices = crosstab_result.iloc[:t].sum()
            if include_total :
                top_indices = crosstab_result.iloc[1:t+1].sum()
            
            top_name = f'Top {t}'
            top_cols.append(top_name)
            top_indices.name = top_name
            top_result.append(pd.DataFrame([top_indices]))
        
        top_indices = pd.concat(top_result)

    med_cols = []
    
    if (medium_auto_flag) and medium is not None :
        if isinstance(medium, bool) and medium :
            top_list = top
            if isinstance(top, int) :
                top_list = [top]

            bot_list = bottom
            if isinstance(bottom, int) :
                bot_list = [bottom]

            vtop = min(top_list)
            vbot = min(bot_list)
            
            if include_total :
                vbot += 1
        
            medium_index = crosstab_result.iloc[vbot:-vtop].index.to_list()
            if medium_index :
                medium_indices = crosstab_result.iloc[vbot:-vtop].sum()
                medium_name = 'Medium'
                med_cols.append(medium_name)
                medium_indices.name = medium_name

                medium_indices = pd.DataFrame([medium_indices])
        
        elif isinstance(medium, (int, list)) :
            medium_list = medium
            if isinstance(medium, int) :
                medium_list = [medium]
            
            if medium_list :
                medium_indices = crosstab_result.iloc[medium_list].sum()
                medium_list = [str(x) for x in medium_list]
                medium_txt = ', '.join(medium_list)
                medium_name = f'Medium ({medium_txt})'
                med_cols.append(medium_name)
                medium_indices.name = medium_name

                medium_indices = pd.DataFrame([medium_indices])

    bot_cols = []
    if bottom is not None:
        bot_list = bottom
        if isinstance(bottom, int) :
            bot_list = [bottom]

        bot_list = list(set(bot_list))
        bot_list.sort()

        bot_result = []
        for b in bot_list :
            bottom_indices = crosstab_result.iloc[-b:].sum()
            bot_name = f'Bottom {b}'
            bot_cols.append(bot_name)
            bottom_indices.name = bot_name
            bot_result.append(pd.DataFrame([bottom_indices]))
        
        bottom_indices = pd.concat(bot_result)
    
    dfs_to_concat = []
    if top_cols :
        dfs_to_concat.append(top_indices)
    
    if med_cols :
        dfs_to_concat.append(medium_indices)
    
    if bot_cols :
        dfs_to_concat.append(bottom_indices)

    # dfs_to_concat 리스트에 데이터프레임이 있을 경우에만 concat을 수행합니다
    if dfs_to_concat:
        crosstab_result = pd.concat([crosstab_result] + dfs_to_concat)


    # Reorder to place Total, Top, and Bottom in the correct positions
    final_order = [total_label] if include_total else []
    
    
    final_order += [o for o in original_index_order if not o in final_order]

    for cols in [top_cols, med_cols, bot_cols] :
        if cols :
            for c in cols :
                final_order.append(c)

    crosstab_result = crosstab_result.loc[final_order]
    
    if include_total :
        cols = list(crosstab_result.columns)
        cols = [cols[-1]] + cols[:-1]
        
        crosstab_result = crosstab_result[cols]


    if calc is not None :
        crosstab_result = pd.concat([crosstab_result, calc])

    crosstab_result = crosstab_result.fillna(0)
    if not fill :
        crosstab_result = crosstab_result.loc[(crosstab_result != 0).any(axis=1), (crosstab_result != 0).any(axis=0)]

    def convert_dtype(value):
        if isinstance(value, float):
            return float(value)
        elif isinstance(value, int):
            return int(value)
        else:
            return value

    return crosstab_result

class CrossTabs(pd.DataFrame):
    def __init__(self, crosstab_result: pd.DataFrame):
        super().__init__(crosstab_result)

    def __repr__(self) -> str:
        return super().__repr__()

    def __str__(self) -> str:
        return super().__str__()

    def _repr_html_(self) -> str:
        return super()._repr_html_()

def get_decipher_datamap_json(pid: Union[str, int]) :
    api.login(api_key, api_server)
    json_map = api.get(f"surveys/selfserve/548/{pid}/datamap", format="json")
    return json_map


def decipher_meta(pid: Union[str, int]) :
    json_map = get_decipher_datamap_json(pid)
    variables = json_map["variables"]

    metadata = {}
    for v in variables :
        label = v['label']
        qtype = v['type']

        if qtype == 'single' :
            values = v['values']
            metadata[label] = [{value['value']: value['title']} for value in values]
        
        if qtype == 'multiple' :
            title = v['title'].split('-')[0].strip()
            metadata[label] = title
        
        if qtype in ['number', 'text'] :
            title = v['title']
            metadata[label] = title

    return metadata


def clean_text(text):
    if text is None :
        return None 

    pattern = r'\(.*?\)'  # 괄호를 포함한 텍스트를 찾기 위한 정규식 패턴
    matches = re.findall(pattern, text)
    if matches :
        clean_text = text.replace(matches[-1], '').strip()
        if clean_text in text :
            return clean_text
        else :
            return text # 괄호가 중간에 있는 것이 아님
    
    return text.strip()

def decipher_title(pid: Union[str, int]) :
    json_map = get_decipher_datamap_json(pid)
    variables = json_map["variables"]
    questions = json_map["questions"]

    title_data = {}
    for v in variables :
        label = v['label']
        qtype = v['type']
        qtitle = v['qtitle']
        row_title = clean_text(v['rowTitle'])
        col_title = clean_text(v['colTitle'])
        
        filt_question = [x for x in questions if x['qlabel']==label]
        if filt_question :
            ques = filt_question[0]
            if 'dq' in ques.keys() :
                if ques['dq'] == 'atmtable' :
                    qtype = 'rating'
        
        title_data[label] = {
            'type' : qtype,
            'title': qtitle,
            'row_title': row_title,
            'col_title': col_title
        }


    return title_data