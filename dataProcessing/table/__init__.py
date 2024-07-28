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
from matplotlib.colors import LinearSegmentedColormap
from langchain.chat_models import ChatOllama
from langchain_openai import ChatOpenAI
from langchain_experimental.llms.ollama_functions import OllamaFunctions
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.schema.runnable import RunnablePassthrough


def custom_calc(df: pd.DataFrame, 
                index: str, 
                columns: Optional[Union[str, List[str]]]=None,
                total_label: str = 'Total',
                aggfunc: Union[str, List[str]] = ['mean']) -> pd.DataFrame:
    """
    Calculates descriptive statistics for the specified index column based on the values of the columns parameter.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    index (str): The column name to group by and calculate statistics for.
    columns (Union[str, List[str]]): The column name(s) to use for grouping.
    aggfunc (Union[str, List[str]]): The aggregation function(s) to apply. Default is 'mean'.
    agg_round (int): Number of decimal places to round the results to. Default is 2.

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
            return value

    if columns is not None :
        if isinstance(columns, str):
            # Single column case
            values = df[columns].value_counts().index.to_list()

            for v in values:
                desc = df[df[columns] == v][index].describe().to_dict()
                for af in aggfunc:
                    ndf.loc[af, str(v)] = set_value(desc[af])
            
            # Total
            desc = df[~df[columns].isna()][index].describe().to_dict()
            for af in aggfunc:
                ndf.loc[af, total_label] = set_value(desc[af])
            
        elif isinstance(columns, list):
            # Binary data case
            for col in columns:
                desc = df[(~df[col].isna()) & (df[col] != 0)][index].describe().to_dict()
                for af in aggfunc:
                    ndf.loc[af, col] = set_value(desc[af])
            
            # Total
            desc = df[((~df[columns].isna()).any(axis=1)) & ((df[columns] != 0).any(axis=1))][index].describe().to_dict()
            for af in aggfunc:
                ndf.loc[af, total_label] = set_value(desc[af])
    else :
        desc = df[index].describe().to_dict()
        for af in aggfunc:
            ndf.loc[af, total_label] = set_value(desc[af])
            
    return ndf


def create_crosstab(df: pd.DataFrame,
                    index: Union[str, List[str]],
                    columns: Optional[Union[str, List[str]]] = None,
                    index_meta: Optional[List[Dict[str, str]]] = None,
                    columns_meta: Optional[List[Dict[str, str]]] = None,
                    fill: bool = True,
                    qtype: Optional[str] = None,
                    score: Optional[int] = None,
                    top: Optional[Union[int, List[int]]] = None,
                    medium: Optional[Union[int, List[int], bool]] = True,
                    bottom: Optional[Union[int, List[int]]] = None,
                    aggfunc: Optional[list] = None,
                    reverse_rating: Optional[bool]=False,
                    total_label: str = 'Total',
                    all_label: str = 'Total',
                    count_label: str = 'Count',
                    conversion: bool = True) -> pd.DataFrame:
    """
    Creates a crosstab from the provided DataFrame with optional metadata for reordering and relabeling indices and columns, and with options to include top/bottom summaries and index sorting.
    
    Parameters:
        df (pd.DataFrame): The input DataFrame.
        index (str or list): The column name or list of column names to use for the crosstab index.
        columns (str or list, optional): The column name or list of column names to use for the crosstab columns.
        index_meta (list of dict, optional): Metadata for the index values and labels.
        columns_meta (list of dict, optional): Metadata for the columns values and labels.
        top (int, optional): Number of top rows to summarize.
        bottom (int, optional): Number of bottom rows to summarize.
    
    Returns:
        pd.DataFrame: The resulting crosstab with optional reordering, relabeling, top/bottom summaries, and total sum row.
    """

    def extract_order_and_labels(metadata: Union[list, dict], front_variable: Optional[list] = None, back_variable: Optional[list] = None):
        """
        Extracts the order and labels from the provided metadata.
        
        Parameters:
            metadata (list of dict): The metadata to extract order and labels from.
            front_variable, back_variable : All / Total
        
        Returns:
            order (list): The extracted order of keys.
            labels (list): The extracted labels for the keys.
        """
        order = [list(d.keys())[0] for d in metadata]
        if front_variable is not None :
            order = front_variable + order
        
        if back_variable is not None :
            order = order + back_variable
        
        labels = [list(d.values())[0] for d in metadata]
        if front_variable is not None :
            labels = front_variable + labels
        
        if back_variable is not None :
            labels = labels + back_variable
        
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

    def create_binary_crosstab(df, index_cols, columns_col=None):
        """
        Creates a crosstab for binary columns in the provided DataFrame.

        Parameters:
            df (pd.DataFrame): The input DataFrame.
            index_cols (list): The list of column names to use for the crosstab index.
            columns_col (str or list, optional): The column name or list of column names to use for the crosstab columns.

        Returns:
            pd.DataFrame: The resulting binary crosstab.
        """
        def count_binary_values(idx, col):
            return (((df[idx] != 0) & (df[col] != 0) & (df[idx].notna()) & (df[col].notna()))).sum()

        def count_values_mixed(sa, ma_index, ma_cols) :
            return ((df[sa] != 0) & (df[ma_cols] == ma_index) & df[sa].notna()).sum()
    
        if columns_col is None:
            # Create a crosstab with a single "Count" column if no columns_col is provided
            crosstab_result = pd.DataFrame(index=index_cols, columns=[count_label])
            for idx in index_cols:
                cnt = ((df[idx] != 0) & (df[idx].notna())).sum()
                crosstab_result.loc[idx, count_label] = cnt
        else:
            if isinstance(index_cols, str) and isinstance(columns_col, list) :
                # Extract unique values from the single column
                unique_cols = df[columns_col].dropna().unique()
                # Create a crosstab with the unique columns
                crosstab_result = pd.DataFrame(index=index_cols, columns=unique_cols)
                for idx in index_cols:
                    for col in unique_cols:
                        cnt = count_values_mixed(col, idx, index_cols)
                        crosstab_result.loc[idx, col] = cnt

            if isinstance(index_cols, list) and isinstance(columns_col, str) :
                # Extract unique values from the single column
                unique_cols = df[columns_col].dropna().unique()
                # Create a crosstab with the unique columns
                crosstab_result = pd.DataFrame(index=index_cols, columns=unique_cols)
                for idx in index_cols:
                    for col in unique_cols:
                        cnt = count_values_mixed(idx, col, columns_col)
                        crosstab_result.loc[idx, col] = cnt
                            
            elif isinstance(index_cols, list) and isinstance(columns_col, list):
                # Create a DataFrame to hold the crosstab result
                crosstab_result = pd.DataFrame(index=index_cols, columns=columns_col)
                
                for idx in index_cols:
                    for col in columns_col:
                        cnt = ((df[idx] != 0) & (df[col] != 0) & (df[idx].notna()) & (df[col].notna())).sum()
                        crosstab_result.loc[idx, col] = cnt

            else:
                raise ValueError("columns_col must be either a string or a list of strings.")
        
        return crosstab_result
    

    # Determine if we are working with single or multiple columns for index
    if isinstance(index, str):
        index_is_binary = False
    elif isinstance(index, list):
        index_is_binary = True
    else:
        raise ValueError("Index must be either a string or a list of strings.")
    

    # if not mode in ["count", "ratio", "both"]:
    #     raise ValueError("Mode must be either 'count', 'ratio', or 'both'")

    # Create the appropriate crosstab
    # Number Only Mean/Min/Max
    if qtype == 'number' :
        num_index = pd.Index(index) if isinstance(index, list) else pd.Index([index])
        if columns is None :
            crosstab_result = pd.DataFrame(index=num_index, columns=[total_label])
        else :
            crosstab_result = pd.DataFrame(index=num_index, 
                                           columns=columns if isinstance(columns, list) else [columns])
    else :
        if columns is None:
            if index_is_binary:
                # Create frequency table for binary columns
                crosstab_result = pd.DataFrame(index=index, columns=[total_label])
                for idx in index:
                    crosstab_result.loc[idx, total_label] = ((df[idx]!=0) & (~df[idx].isna())).sum()

            else:
                crosstab_result = df[index].value_counts().to_frame(name=total_label)
        else:
            if isinstance(columns, str):
                columns_is_binary = False
            elif isinstance(columns, list):
                columns_is_binary = True
            else:
                raise ValueError("Columns must be either a string or a list of strings.")
            
            if index_is_binary and columns_is_binary:
                crosstab_result = create_binary_crosstab(df, index, columns)
            elif index_is_binary:
                crosstab_result = create_binary_crosstab(df, index, columns)
            elif columns_is_binary:
                crosstab_result = create_binary_crosstab(df, columns, index).T
            else:
                crosstab_result = pd.crosstab(
                    index=df[index],
                    columns=df[columns],
                )
        
        
    base_index = crosstab_result.index
    base_columns = crosstab_result.columns

    # Original Order
    back_index = [] # crosstab result back variables 

    # Add top and bottom summaries if needed
    if (qtype == 'rating') and (not index_meta) and (score is None) :
        raise ValueError("If qtype is 'rating', score or index_meta must be provided.")

    # Rating Type 
    if qtype == 'rating' :
        if score is None :
            score = max([int(list(x.keys())[0]) for x in index_meta])
        
        scores = [i for i in range(1, score+1)]
        for idx in scores :
            if idx not in base_index :
                crosstab_result.loc[idx] = 0

        crosstab_result = crosstab_result.sort_index(ascending=reverse_rating)
        base_index = crosstab_result.index.to_list()

    # Total Setting
    if isinstance(index, str) :
        if columns is None :
            crosstab_result.loc[all_label, total_label] = (~df[index].isna()).sum()

        if isinstance(columns, str) :
            crosstab_result.loc[all_label, :] = pd.Series({col: ((df[columns]==col) & (~df[index].isna())).sum() for col in base_columns})
            crosstab_result.loc[:, total_label] = pd.Series({idx: (df[index]==idx).sum() for idx in base_index})
            crosstab_result.loc[all_label, total_label] = ((~df[index].isna()) & (~df[columns].isna())).sum()
            
        if isinstance(columns, list) :
            crosstab_result.loc[all_label, :] = pd.Series({col: ((~df[index].isna()) & (~df[col].isna()) & (df[col]!=0)).sum() for col in base_columns})
            crosstab_result.loc[:, total_label] = pd.Series({idx: (df[index]==idx).sum() for idx in base_index})
            crosstab_result.loc[all_label, total_label] = ((~df[index].isna()) & ((df[columns]!=0).any(axis=1)) & ((~df[columns].isna()).any(axis=1))).sum()
        
    if isinstance(index, list) :
        if columns is None :
            crosstab_result.loc[all_label, total_label] = (((df[index]!=0).any(axis=1)) & ((~df[index].isna()).any(axis=1))).sum()
        
        if isinstance(columns, str) :
            crosstab_result.loc[:, total_label] = pd.Series({idx: ((~df[idx].isna()) & (df[idx]!=0) & (~df[columns].isna())).sum() for idx in base_index})
            crosstab_result.loc[all_label, :] = pd.Series({col: ((df[columns]==col) & (df[index]!=0).any(axis=1) & (~df[index].isna()).any(axis=1)).sum() for col in base_columns})
            crosstab_result.loc[all_label, total_label] = ((~df[columns].isna()) & ((df[index]!=0).any(axis=1)) & ((~df[index].isna()).any(axis=1))).sum()
    
        if isinstance(columns, list) :
            crosstab_result.loc[:, total_label] = pd.Series({idx: ((~df[idx].isna()) & (df[idx]!=0)).sum() for idx in base_index})
            crosstab_result.loc[all_label, :] = pd.Series({col: ((~df[col].isna()) & (df[col]!=0) & ((df[index]!=0).any(axis=1)) & ((~df[index].isna()).any(axis=1))).sum() for col in base_columns})
            crosstab_result.loc[all_label, total_label] = (((df[index]!=0).any(axis=1)) &( (~df[index].isna()).any(axis=1)) &( (df[columns]!=0).any(axis=1)) & ((~df[columns].isna()).any(axis=1))).sum()

    # ALL/TOTAL ORDER SETTING
    # crosstab_result.index = [all_label] + original_index_order
    # crosstab_result.columns = [total_label] + original_columns_order
    

    medium_auto_flag = False
    if all([n is not None for n in [top, bottom]]) and (medium is not False) :
        medium_auto_flag = True

    top_cols = []
    if top is not None:
        chk_top_list = top
        if isinstance(top, int) :
            chk_top_list = [top]
        
        top_list = [] # Duplicate remove
        for t in chk_top_list :
            if not t in top_list :
                top_list.append(t)

        top_result = []
        for t in top_list :
            top_indices = crosstab_result.loc[base_index[:t]].sum()
            
            top_name = f'Top {t}'
            top_cols.append(top_name)
            back_index.append(top_name)
            top_indices.name = top_name
            top_result.append(pd.DataFrame([top_indices]))
        
        top_indices = pd.concat(top_result)

    med_cols = []
    if (medium_auto_flag) and medium is not None :
        if isinstance(medium, bool) and medium :
            # TOP
            chk_top_list = top
            if isinstance(top, int) :
                chk_top_list = [top]
            
            top_list = [] # Duplicate remove
            for t in chk_top_list :
                if not t in top_list :
                    top_list.append(t)

            # BOTTOM
            chk_bot_list = bottom
            if isinstance(bottom, int) :
                chk_bot_list = [bottom]
            
            bot_list = [] # Duplicate remove
            for b in chk_bot_list :
                if not b in bot_list :
                    bot_list.append(b)

            vtop = min(top_list)
            vbot = min(bot_list)
                    
            medium_index = crosstab_result.loc[base_index[vbot:-vtop]].index.to_list()
            if medium_index :
                medium_indices = crosstab_result.loc[base_index[vbot:-vtop]].sum()
                medium_name = 'Medium'
                med_cols.append(medium_name)
                back_index.append(medium_name)
                medium_indices.name = medium_name

                medium_indices = pd.DataFrame([medium_indices])
        
        elif isinstance(medium, (int, list)) :
            medium_list = medium
            if isinstance(medium, int) :
                medium_list = [medium]
            
            if medium_list :
                medium_indices = crosstab_result.loc[[x for x in base_index if x in medium_list]].sum()
                medium_list = [str(x) for x in medium_list]
                medium_name = f'Medium'
                if len(medium_list) >= 2 :
                    medium_txt = '/'.join(medium_list)
                    medium_name = f'Medium [{medium_txt}]'
                
                med_cols.append(medium_name)
                back_index.append(medium_name)
                medium_indices.name = medium_name

                medium_indices = pd.DataFrame([medium_indices])

    bot_cols = []
    if bottom is not None:
        chk_bot_list = bottom
        if isinstance(bottom, int) :
            chk_bot_list = [bottom]
        
        bot_list = [] # Duplicate remove
        for b in chk_bot_list :
            if not b in bot_list :
                bot_list.append(b)

        bot_list = list(set(bot_list))
        
        bot_result = []
        for b in bot_list :
            bottom_indices = crosstab_result.loc[base_index[-b:]].sum()
            bot_name = f'Bottom {b}'
            bot_cols.append(bot_name)
            back_index.append(bot_name)
            bottom_indices.name = bot_name
            bot_result.append(pd.DataFrame([bottom_indices]))
        
        bottom_indices = pd.concat(bot_result)
    
    crosstab_result.index = crosstab_result.index.map(str)
    crosstab_result.columns = crosstab_result.columns.map(str)

    # Netting
    dfs_to_concat = []
    if top_cols :
        dfs_to_concat.append(top_indices)
    
    if med_cols :
        dfs_to_concat.append(medium_indices)
    
    if bot_cols :
        dfs_to_concat.append(bottom_indices)

    if dfs_to_concat:
        net_result = pd.concat(dfs_to_concat)
        net_result.index = net_result.index.map(str)
        net_result.columns = net_result.columns.map(str)

        crosstab_result = pd.concat([crosstab_result, net_result])

    
    index_order = [all_label] + [i for i in crosstab_result.index.to_list() if not i == all_label]
    column_order = [total_label] + [i for i in crosstab_result.columns.to_list() if not i == total_label]
    
    crosstab_result = crosstab_result.reindex(index_order)
    crosstab_result = crosstab_result[column_order]

    if not qtype in ['number'] :
        crosstab_result = crosstab_result.astype(int)

    # if not qtype in ['number'] :
    #     if mode in ['count'] :
    #         crosstab_result = crosstab_result.astype(int)
            
    #     elif mode in ['ratio'] :
    #         crosstab_result = crosstab_result.astype(float)
    #         all_value = crosstab_result.iloc[0]
    #         crosstab_result.iloc[1:, :] = (crosstab_result.iloc[1:, :].div(all_value))*100
    #         crosstab_result = crosstab_result.round(ratio_round)
    #         crosstab_result = crosstab_result.map(lambda x: f"{x:.{ratio_round}f}")
    #         crosstab_result = crosstab_result.astype(str)
    #         crosstab_result.iloc[0] = crosstab_result.iloc[0].apply(lambda x: str(int(float(x))))
    #         if ratio_round == 0 :
    #             crosstab_result = crosstab_result.map(lambda x: int(x.replace('.0', '')) if not x == 'nan' else 0)
            
    #     elif mode in ['both'] :
    #         crosstab_result = crosstab_result.astype(float)
    #         all_value = crosstab_result.iloc[0]

    #         def transform_value(x, col):
    #             if x == 0:
    #                 return '0'
    #             if ratio_round == 0:
    #                 calc = round(x / all_value[col] * 100, ratio_round)
    #                 calc = f"{calc:.{ratio_round}f}"
    #                 calc = str(calc).replace('.0', '')
    #                 return f'{int(x)} ({calc}%)'
    #             else:
    #                 calc = round(x / all_value[col] * 100, ratio_round)
    #                 calc = f"{calc:.{ratio_round}f}"
    #                 return f'{int(x)} ({calc}%)'

    #         # crosstab_result의 나머지 부분을 문자열로 변환합니다
    #         crosstab_result = crosstab_result.astype(str)
    #         crosstab_result.iloc[0] = crosstab_result.iloc[0].apply(lambda x: str(int(float(x))))
    #         crosstab_result.iloc[1:, :] = crosstab_result.iloc[1:, :].apply(lambda col: col.apply(lambda x: transform_value(float(x), col.name)))

    # Calc
    calc = None
    if aggfunc is not None :
        back_index += aggfunc
        calc = custom_calc(df, 
                           index=index, 
                           columns=columns, 
                           aggfunc=aggfunc, 
                           total_label=total_label)
        calc.index = calc.index.map(str)
        calc.columns = calc.columns.map(str)

    if calc is not None :
        crosstab_result = pd.concat([crosstab_result, calc])
        # crosstab_result.iloc[0] = crosstab_result.iloc[0].apply(lambda x: str(int(float(x))))

        if conversion :
            if qtype == 'rating' and 'mean' in crosstab_result.index.to_list() :
                conversion_index = '100 point conversion'
                crosstab_result.loc[conversion_index, :] = [0 if i == 0 else ((i-1)/(score-1))*100 for i in crosstab_result.loc['mean', :].values]
                back_index.append(conversion_index)

    # Process index metadata
    if index_meta :
        index_order, index_labels = extract_order_and_labels(index_meta, [all_label], back_index)
        crosstab_result = add_missing_indices(crosstab_result, index_order)
        crosstab_result = reorder_and_relabel(crosstab_result, index_order, index_labels, axis=0, name=None)

    # Process columns metadata
    if columns_meta:
        columns_order, columns_labels = extract_order_and_labels(columns_meta, [total_label])
        crosstab_result = add_missing_indices(crosstab_result.T, columns_order).T
        crosstab_result = reorder_and_relabel(crosstab_result, columns_order, columns_labels, axis=1, name=None)
    

    if qtype in ['number'] : 
        crosstab_result = crosstab_result.loc[[all_label, *aggfunc], :]
        # if index_name is not None and index_name is not False :
        #     crosstab_result.index.name = index_name

    # crosstab_result = crosstab_result.fillna(0).infer_objects(copy=False)

    if not fill :
        crosstab_result = crosstab_result.loc[(crosstab_result != 0).any(axis=1), (crosstab_result != 0).any(axis=0)]

    
    return crosstab_result



class CrossTabs(pd.DataFrame):
    def __init__(self, crosstab_result: pd.DataFrame):
        super().__init__(crosstab_result)

    def __repr__(self) -> str:
        return super().__repr__()

    def __str__(self) -> str:
        return super().__str__()

    def _repr_html_(self) -> str:
        # result = self.copy()
        # mask = ~result.index.isin([idx for idx in result.index if idx[-1] in ['mean', 'man', 'min', 'std', 'Total']])
        # result = result.round(0)
        return super()._repr_html_()
    
    def ratio(self, ratio_round: Optional[int] = 0, heatmap: bool = True, post_text:Optional[str] = None) -> pd.DataFrame:

        if ratio_round is not None and ratio_round < 0 :
            raise ValueError('ratio_round must be greater than 0')

        result = self.astype(float)
        all_value = result.iloc[0]

        mask_index = ['mean', 'man', 'min', 'max', 'std', '100 point conversion', 'Total']
        if isinstance(result.index, pd.MultiIndex) :
            mask = ~result.index.isin([idx for idx in result.index if idx[-1] in mask_index])
        else :
            mask = ~result.index.isin(mask_index)
        
        result.loc[mask, :] = (result.loc[mask, :].div(all_value)) * 100
        if ratio_round is not None :
            result.loc[mask, :] = result.loc[mask].round(ratio_round)

        if heatmap :
            cmap = LinearSegmentedColormap.from_list("custom_blue", ["#ffffff", "#2d6df6"])
            styled_result = result.style.map(
                lambda val: 'background-color: #ffffff' if np.isnan(val) else '', 
                subset=pd.IndexSlice[~mask, :]
            ).background_gradient(
                cmap=cmap,
                subset=pd.IndexSlice[mask, :],
                vmin=0, vmax=100
            )

            format_string = "{:.0f}"
            if ratio_round is not None :
                format_string = "{:." + str(ratio_round) + "f}"
            if post_text is not None :
                format_string = format_string + post_text
            
            include_total_index = []
            with_total = [i for i in mask_index if not i in ['Total']]
            if isinstance(result.index, pd.MultiIndex) :
                include_total_index = ~result.index.isin([idx for idx in result.index if idx[-1] in with_total])
            else :
                include_total_index = ~result.index.isin(with_total)
            
            styled_result = styled_result.format(format_string, subset=pd.IndexSlice[include_total_index, :])
            return styled_result

        return result
    
    def chat_ai(self, 
                model: Literal['gpt-4o', 'gpt-4o-mini', 'llama3', 'llama3.1'] = 'gpt-4o-mini',
                sub_title: Optional[str] = None,
                with_table: Optional[bool] = False,
                table_type: Optional[Literal['single', 'rating', 'rank', 'multiple', 'number', 'text']] = None,
                prompt: Optional[str] = None,
                lang: Optional[Literal['korean', 'english']] = 'korean'):
        
        if model not in ['gpt-4o', 'gpt-4o-mini', 'llama3', 'llama3.1'] :
            raise ValueError('model must be gpt-4o, gpt-4o-mini, llama3, llama3.1')
        
        if lang not in ['korean', 'english'] :
            raise ValueError('lang must be korean or english')

        llm = None
        if model in ['llama3', 'llama3.1'] :
            llm = ChatOllama(
                model=model,
                temperature=0.1)
        
        if model in ['gpt-4o', 'gpt-4o-mini'] :
            llm = ChatOpenAI(
                    temperature=0.1,
                    model='gpt-4o-mini')
        
        post_text = '%'
        default_prompt = F"""
User Persona: "Professional Data Analyst"
User Goal: "Analyze and summarize cross-tabulation results"
User Task: "Includes basic statistics, if available, to provide a summary of the analysis and insights beyond the total number of responses, focusing on trends and noteworthy points."
Report Language: "{lang.upper()}"

Prompt:
You are a professional data analyst. Your task is to analyze and summarize the given cross-tabulation results. Follow these steps:

Exclude any analysis on the total response count.
Focus on analyzing by group in each row in each column of the cross table.
If the cross-tabulation includes basic statistics like mean, min, and max, provide an analysis of these as well.
Derive comprehensive insights and summarize them.
The final report should be written in {lang.upper()} and in complete sentences.
Take a deep breath and let's work this out in a step by step way to be sure we have the right answer.
"""
        
        if (isinstance(table_type, str) and table_type in ['number', 'text']) or (isinstance(table_type, list) and any(t in ['number', 'text'] for t in table_type)) :
            post_text = None
            default_prompt = f"""
User Persona: "Professional Data Analyst"
User Goal: "Analyze and summarize cross-tabulation results"
User Task: "Provide detailed analysis and insights excluding total response counts, focusing on calculated basic statistics per column, and deliver the final report in {lang.upper()}"
Report Language: "{lang.upper()}"

Prompt:
You are a professional data analyst. Your task is to analyze and summarize the given cross-tabulation results. Follow these steps:

Exclude any analysis on the total response count.
Focus on analyzing the calculated basic statistics (e.g., mean, min, max) for each column.
Derive comprehensive insights and summarize them.
The final report should be written in {lang.upper()} and in complete sentences.
Take a deep breath and let's work this out in a step by step way to be sure we have the right answer.
"""

        if prompt is None :
            prompt = default_prompt
        
        crosstab = self.ratio(ratio_round=2, heatmap=False, post_text=post_text)
        crosstab = crosstab.to_markdown()

        prompt_template = ChatPromptTemplate.from_template(
"""
{prompt}
===
{sub_title}
{crosstab}
"""
)
        try :
            chain = prompt_template | llm
            chat_content = chain.invoke({
                            'prompt': prompt,
                            'crosstab': crosstab,
                            'sub_title': f'[`{sub_title}` CROSSTAB Result]' if sub_title is not None else '[CROSSTAB]',
                        })
        except Exception as e :
            print(e)
            return None
        
        chat_result = chat_content.content

        if with_table :
            ratio_table = self.ratio(ratio_round=0)
            display(HTML(ratio_table.to_html()))
        
        return chat_result
        


def clean_text(text):
    if text is None :
        return None 

    pattern = r'\(.*?\)'  # 괄호를 포함한 텍스트를 찾기 위한 정규식 패턴
    matches = re.findall(pattern, text)
    if matches :
        clean_text = text.replace(matches[-1], '').strip()
        if clean_text == '' :
            return text
    
        if clean_text in text :
            return clean_text
        else :
            return text # 괄호가 중간에 있는 것이 아님
    
    return text.strip()

def get_decipher_datamap(pid: Union[str, int], map_format: Literal['json', 'json_stacked', 'html', 'text', 'tab', 'xlsx', 'fw', 'fw', 'fw', 'cb', 'cb', 'cb', 'uncle', 'sss', 'sas', 'quantum', 'spss_fw', 'spss_tab', 'netmr', 'netmr']='json') :
    api.login(api_key, api_server)
    url = f"surveys/selfserve/548/{pid}/datamap"

    decipher_map = api.get(url, format=map_format)
    return decipher_map


rank_flag = ['1순위', '2순위', '1st', '2nd']

def decipher_meta(pid: Union[str, int]) :
    json_map = get_decipher_datamap(pid)
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
    json_map = get_decipher_datamap(pid)
    questions = json_map["questions"]

    title_data = {}
    for v in questions :
        label = v['qlabel']
        variables = v['variables']
        qtype = v['type']
        qtitle = v['qtitle']
        grouping = v['grouping']
        for i in variables :
            sub_title = None
            vlabel = i['label']
            itype = i['type']
            if grouping == 'rows' :
                sub_title = clean_text(i['rowTitle'])
            
            if grouping == 'columns' :
                sub_title = clean_text(i['colTitle'])
            
            col_list = [v['colTitle'] for v in variables]
            if not itype in ['text'] and any(col in rank_flag for col in col_list) :
                qtype = 'rank'
            
            if not qtype in ['text'] and itype in ['text'] :
                qtype = 'other_open'
                
            if 'dq' in v.keys() :
                if v['dq'] == 'atmtable' :
                    qtype = 'rating'
                if v['dq'] == 'ranksort' :
                    qtype = 'ranksort'

            title_data[vlabel] = {
                'type' : qtype,
                'title': qtitle,
                'sub_title': sub_title,
            }


    return title_data


def decipher_map(pid: Union[str, int]) :
    json_map = get_decipher_datamap(pid)
    questions = json_map["questions"]

    return_questions = []
    for q in questions :
        qlabel = q['qlabel']
        qtype = q['type']
        title = q['qtitle']
        variables = q['variables']
        label_list = [{v['label']: {'vgroup': v['vgroup'], 'rowTitle': clean_text(v['rowTitle']), 'colTitle': clean_text(v['colTitle'])}} for v in variables]
        value_list = []
        meta_list = []
        oe_variables = []
        grouping = q['grouping']
        if not qtype in ['text'] :
            oe_variables = [{'qlabel': v['label'], \
                            'type': 'other_open', \
                            'row': v['row'], \
                            'col': v['col'], \
                            'variables': [{v['label']: {'vgroup': qlabel, 'rowTitle': clean_text(v['rowTitle']), 'colTitle': clean_text(v['colTitle'])}}],\
                            'title': title,\
                            'grouping': grouping, \
                            'meta': [{v['label']: v['title']}],\
                            } for v in variables if v['type']=='text']
            label_list = [{v['label']: {'vgroup': v['vgroup'], 'rowTitle': clean_text(v['rowTitle']), 'colTitle': clean_text(v['colTitle'])}} for v in variables if v['type'] in ['single', 'multiple', 'number']]

        if 'values' in q.keys():
            values = q['values']
            value_list = [x['value'] for x in values]
            meta_list = [{x['value']: x['title']} for x in values]
        else :
            value_list = [v['value'] for v in variables if 'value' in v.keys()]
            meta_list = [{x['label']: { \
                            'value': x['value'] if 'value' in x.keys() else None, \
                            'rowTitle': clean_text(x['rowTitle']), \
                            'colTitle': clean_text(x['colTitle'])}} for x in variables]
        
        if 'dq' in q.keys() :
            if q['dq'] == 'atmtable' :
                qtype = 'rating'
            
        col_list = [v['colTitle'] for v in variables]
        if any(col in rank_flag for col in col_list) :
            qtype = 'rank'
        
        # if qtype == 'multiple' :
        multiples = [v for v in variables if v['type'] != 'text']
        vgroups = [v['vgroup'] for v in multiples if not v['vgroup'] == qlabel]
        if vgroups :
            groups = []
            for gr in vgroups :
                if not gr in groups :
                    groups.append(gr)
            
            for gr in groups :
                filt_variable = [v for v in multiples if v['vgroup'] == gr]
                ma_label_list = [{v['label']: {'rowTitle': clean_text(v['rowTitle']), 'colTitle': clean_text(v['colTitle']), 'vgroup': v['vgroup']}} for v in filt_variable]
                ma_values = [v['value'] for v in filt_variable if 'value' in v.keys()]
                ma_meta = [{x['label']: { \
                            'value': x['value'] if 'value' in x.keys() else None, \
                            'rowTitle': clean_text(x['rowTitle']), \
                            'colTitle': clean_text(x['colTitle'])}} for x in filt_variable]
                return_questions.append({
                    'qlabel': gr, \
                    'variables': ma_label_list,
                    'type': qtype, \
                    'values': ma_values, \
                    'meta': ma_meta, \
                    'grouping': grouping, \
                    'title': title
                })
        else : 
            return_questions.append({'qlabel': qlabel, \
                                    'variables': label_list, \
                                    'values': value_list, \
                                    'type': qtype, \
                                    'meta': meta_list, \
                                    'grouping': grouping, \
                                    'title': title})

        
        if oe_variables :
            for oe in oe_variables :
                return_questions.append(oe)
            
    return return_questions


def decipher_create_layout(pid: Union[str, int], base_layout: str = 'DoNotDelete', qids: Optional[dict]=None) :
        api.login(api_key, api_server)
        survey = f'selfserve/548/{pid}'
        url = f'surveys/{survey}/layouts'
        layout = api.get(url)

        maps = [m for m in layout if m['description'] == base_layout]
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
        
        ce = ""
        oe = ""

        for label, width in [ ('record', 7), ('uuid', 16), ('UID', 16)]:
            write_text = f'{label},{label},{width}\n'
            ce += write_text
            oe += write_text

        resp_chk = [v for v in variables if v['label'] == 'RespStatus']
        if resp_chk :
            ce += f'RespStatus,RespStatus,8\n'

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
                        if qids is not None :
                            verify_check = [attr['value'].split('-')[1] for ql, attr in list(qids.items()) if (ql == qlabel) or (ql == label)]
                            if verify_check :
                                max_width = len(verify_check[0])
                                    # print(label, verify_check, max_width)
                                if qtype == 'float' :
                                    max_width += 4
                                write_text = f'{label},{altlabel},{max_width}\n'
                        else :
                            write_text = f'{label},{altlabel},19\n'
                    ce += write_text
                if qtype in oe_vars :
                    oe += write_text

        oe += f'decLang,decLang,60\n'

        return {
            'CE': ce,
            'OE': oe
        }


def get_decipher_data(pid: Union[str, int], data_format: Literal['tab', 'fwu', 'fw', 'flat', 'flat_all', 'pipe', 'csv', 'cb', 'json', 'spss', 'spss16', 'spss15', 'spss16_oe', 'spss_data'] = 'xlsx', cond: str = 'qualified', layout: Optional[Union[str, int]] = None) :
    api.login(api_key, api_server)
    survey = f'selfserve/548/{pid}'
    url = f'surveys/{survey}/data'
    decipher_data = api.get(url, format=data_format, cond=cond, layout=layout)

    return decipher_data
    