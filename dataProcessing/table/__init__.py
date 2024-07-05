import pandas as pd
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
from decipherAutomatic.dataProcessing.dataCheck import *
from pandas.io.formats import excel
import zipfile

def create_crosstab(df: pd.DataFrame,
                    index: Union[str, List[str]],
                    columns: Optional[Union[str, List[str]]] = None,
                    index_meta: Optional[List[Dict[str, str]]] = None,
                    columns_meta: Optional[List[Dict[str, str]]] = None,
                    include_total: bool = False,
                    index_name: Optional[str] = None,
                    columns_name: Optional[str] = None,
                    top: Optional[int] = None,
                    bottom: Optional[int] = None,
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
            if idx not in df.index:
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
            df.index = pd.Index(labels, name=name)
        else:
            df = df[order]
            df.columns = pd.Index(labels, name=name)
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
            crosstab_result = pd.DataFrame(index=index_cols, columns=["Count"])
            for idx in index_cols:
                count = count_binary_values(idx, idx)
                crosstab_result.loc[idx, "Count"] = count
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
                        crosstab_result.loc[total_label, col] = ma_ma_count(index_cols, col)
                    
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
            crosstab_result = pd.DataFrame(index=index, columns=["Count"])
            for idx in index:
                crosstab_result.loc[idx, "Count"] = (df[idx] != 0).sum()
        else:
            crosstab_result = df[index].value_counts().to_frame(name="Count")
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

    # Sort index if sort_index is specified
    original_index_order = crosstab_result.index.to_list()

    if any([n is not None for n in [top, bottom]]) :
        sort_index = 'desc'

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
    if top is not None:
        top_indices = crosstab_result.iloc[:top].sum()
        if include_total :
            top_indices = crosstab_result.iloc[1:top+1].sum()
        
        top_name = f'Top {top}'
        top_indices.name = top_name

    if bottom is not None:
        bottom_indices = crosstab_result.iloc[-bottom:].sum()
        bot_name = f'Bottom {bottom}'
        bottom_indices.name = bot_name

    if top is not None :
        crosstab_result = pd.concat([crosstab_result, pd.DataFrame([top_indices])])
    
    if bottom is not None :
        crosstab_result = pd.concat([crosstab_result, pd.DataFrame([bottom_indices])])


    # Reorder to place Total, Top, and Bottom in the correct positions
    final_order = [total_label] if include_total else []
    
    
    final_order += [o for o in original_index_order if not o in final_order]

    if top is not None:
        final_order.append(top_name)
    
    if bottom is not None :
        final_order.append(bot_name)

    crosstab_result = crosstab_result.loc[final_order]
    
    if include_total :
        cols = list(crosstab_result.columns)
        cols = [cols[-1]] + cols[:-1]
        
        crosstab_result = crosstab_result[cols]
    
    return crosstab_result

class CrossTabs :
    def __init__(self, crosstab_result:pd.DataFrame) :
        self.result = crosstab_result

    def __getattr__(self, name):
        # Delegate attribute access to the DataCheck instance
        return getattr(self.result, name)

    def __repr__(self) -> str:
        return self.result.__repr__()

    def __str__(self) -> str:
        return self.result.__str__()

    def _repr_html_(self) -> str:
        return self.result._repr_html_()

class DataProcessing :
    def __init__(self, data_check_instance:DataCheck, meta: Optional[Dict[str, Any]] = None, title: Optional[Dict[str, Any]] = None) -> None:
        self.data_check_instance = data_check_instance
        self.meta = meta
        self.title = title

    def __getattr__(self, name):
        return getattr(self.data_check_instance, name)

    def __getitem__(self, variables):
        return self.data_check_instance[variables]

    def setting_meta(self, meta, variable) :
        if variable is None :
            return None
    
        return_meta = None
        if meta is None :
            if self.meta is not None :
                if isinstance(variable, str) :
                    if variable in self.meta.keys() :
                        return_meta = self.meta[variable]
                
                if isinstance(variable, list) :
                    return_meta = [{v: self.meta[v]} if v in self.meta.keys() else {v: ''} for v in variable]
        else :
            return_meta = meta
        
        return return_meta
    
    def setting_title(self, title, variable) :
        if variable is None :
            return None

        return_title = None
        if title is None :
            if self.title is not None :
                chk_var = variable
                if isinstance(chk_var, list) :
                    chk_var = variable[0]
                
                if chk_var in self.title.keys() :
                    return_title = self.title[chk_var]['title']
        else :
            return_title = title

        return return_title

    def table(self, index: Union[str, List[str]],
                    columns: Optional[Union[str, List[str]]] = None,
                    index_meta: Optional[List[Dict[str, str]]] = None,
                    columns_meta: Optional[List[Dict[str, str]]] = None,
                    include_total: bool = False,
                    index_name: Optional[str] = None,
                    columns_name: Optional[str] = None,
                    top: Optional[int] = None,
                    bottom: Optional[int] = None,
                    sort_index: Optional[str] = None) -> pd.DataFrame :

            df = self.copy()

            index_meta = self.setting_meta(index_meta, index)
            index_name = self.setting_title(index_name, index)

            columns_meta = self.setting_meta(columns_meta, columns)
            columns_name = self.setting_title(columns_name, columns)
            
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

def decipher_title(pid: Union[str, int]) :
    json_map = get_decipher_datamap_json(pid)
    variables = json_map["variables"]

    title_data = {}
    for v in variables :
        label = v['label']
        qtype = v['type']
        qtitle = v['qtitle']
        title_data[label] = {
            'type' : qtype,
            'title': qtitle
        }

    
    return title_data


def SetUpDataProcessing(dataframe: pd.DataFrame, keyid: Optional[str]=None, platform: Literal['decipher']=None, pid: Optional[Union[str, int]]=None) :
    module_path = os.path.dirname(__file__)
    css_path = os.path.join(os.path.dirname(module_path), 'dataCheck')
    css = get_css(os.path.join(css_path, 'styles.css'))
    display(HTML(css))
    df = convert_columns_to_nullable_int(dataframe)

    metadata = None
    if platform == 'decipher' :
        if pid is None :
            raise ValueError("Enter Decipher pid")
        
        metadata = decipher_meta(pid)
        title = decipher_title(pid)
    
    dc = DataCheck(df, css=css, keyid=keyid)
    return DataProcessing(dc, meta=metadata, title=title)