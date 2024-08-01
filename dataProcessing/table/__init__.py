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


    def pd_crosstab_check(df, x_name, y_name) :
        tabs = pd.crosstab(
            index=df[x_name],
            columns=df[y_name],
        )

        if (len(tabs.index) > 0) and (len(tabs.columns) > 0) :
            if 0 in tabs.columns :
                tabs.drop(0, axis=1, inplace=True)
            if tabs.columns.to_list() :
                tabs.columns = [y_name]
            return tabs
        else :
            return None

    def binary_crosstab(df, x_name, y_name_list) :
        crosstab_combined = pd.concat(
            [pd_crosstab_check(df, x_name, col) for col in y_name_list if pd_crosstab_check(df, x_name, col) is not None],
            axis=1
        )

        crosstab_combined = crosstab_combined.loc[:,~crosstab_combined.columns.duplicated()]
        
        return crosstab_combined

    def index_signle_total(index) :
        sa = pd.crosstab(
                df[index],
                columns='count',
                margins=True,
                margins_name=total_label,
            )
        
        return sa.loc[:, total_label].to_frame()

    def index_binary_total(index) :
        tabs = []
        for row in index :
            ma = pd.crosstab(
                df[row],
                columns='count',
                margins=True,
                margins_name=total_label
            )

            if 0 in ma.index :
                x = ma.drop(0)
            chk = [i for i in x.index.to_list() if i != total_label]
            if chk :
                rename_dict = {}
                rename_dict[chk[0]] = row
                x.rename(index=rename_dict, inplace=True)
                tabs.append(x)
            else :
                zero_row = pd.DataFrame([0], index=[row], columns=[total_label])
                tabs.append(zero_row)

        ma_table = pd.concat(tabs)
        return ma_table.loc[~ma_table.index.duplicated(), :]


    def calc_number(index, default_calc) :
        default_number = df[index].describe().to_frame()
        default_number.rename(index={'count': total_label}, inplace=True)

        set_index = default_calc
        if aggfunc is not None :
            set_index = aggfunc
        
        set_index = [total_label] + set_index
        default_number = default_number.loc[set_index]
        default_number.columns = pd.Index([total_label])

        return default_number

    # ==== 

    start_time = time.time()
    # print(f"CrossTab Start : {start_time}")

    # Determine if we are working with single or multiple columns for index
    if not isinstance(index, (str, list)):
        raise ValueError("Index must be either a string or a list of strings.")
    

    # Create the appropriate crosstab
    # Number Only Mean/Min/Max
    if qtype in ['number', 'float'] :
        if isinstance(index, list) :
            raise ValueError("Index must be a string for number type.")

    default_calc = ['mean', 'min', 'max']
    agg = {}

    if columns is not None :
        if not isinstance(columns, (str, list)):
            raise ValueError("Columns must be either a string or a list of strings.")


        if qtype in ['number', 'float'] :
            crosstab_result = calc_number(index, default_calc)
            agg[index] = default_calc + ['count']

            if isinstance(columns, list) :
                tabs = []
                for col in columns :
                    x = pd.pivot_table(
                        df,
                        columns=col,
                        values=index,
                        aggfunc=agg
                    )

                    if 0 in x.columns :
                        x = x.drop(0, axis=1)
                    
                    x.rename(index={'count': total_label}, inplace=True)
                    if x.columns.to_list() :
                        x.columns = [col]
                    
                        for idx in x.index :
                            crosstab_result.loc[idx, col] = x.loc[idx, col]

        else :
            # Nomar CrossTab
            if isinstance(index, str) and isinstance(columns, list) :
                index_total = index_signle_total(index)
                columns_total = index_binary_total(columns).T

                crosstab_result = binary_crosstab(df, index, columns)
                for col in columns_total.columns :
                    crosstab_result.loc[total_label, col] = columns_total.loc[total_label, col]

                for idx in index_total.index :
                    crosstab_result.loc[idx, total_label] = index_total.loc[idx, total_label]

            if isinstance(index, list) and isinstance(columns, str) :
                index_total =  index_binary_total(index)
                columns_total = index_signle_total(columns)
                crosstab_result = binary_crosstab(df, columns, index).T

                for idx in index_total.columns :
                    crosstab_result.loc[idx, total_label] = index_total.loc[total_label, idx]

                for col in columns_total.index :
                    crosstab_result.loc[col, total_label] = columns_total.loc[col, total_label]

            if isinstance(index, list) and isinstance(columns, list) :
                index_total =  index_binary_total(index)
                columns_total = index_binary_total(columns).T
                
                finale_table = []
                for col in columns :
                    tabs = []
                    for row in index :
                        x = pd.crosstab(
                            index=df[row],
                            columns=df[col],
                        )

                        if 0 in x.index :
                            x = x.drop(0)
                        
                        if 0 in x.columns :
                            x = x.drop(0, axis=1)
                        
                        idx_list = x.index.to_list()
                        col_list = x.columns.to_list()
                        if idx_list and col_list :
                            rename_index = {}
                            rename_index[idx_list[0]] = row
                            rename_columns = {}
                            rename_columns[col_list[0]] = col
                            x.rename(index=rename_index, columns=rename_columns, inplace=True)
                            tabs.append(x)
                        else :
                            zero_row = pd.DataFrame([0], index=[row], columns=[col])
                            tabs.append(zero_row)

                    ma_table = pd.concat(tabs)
                    if 0 in ma_table.columns :
                        ma_table = ma_table.drop(0, axis=1)
                    
                    if ma_table.columns.to_list() :
                        finale_table.append(ma_table.loc[~ma_table.index.duplicated(), :])

                crosstab_result = pd.concat(finale_table, axis=1)
                crosstab_result = crosstab_result.loc[:, ~crosstab_result.columns.duplicated()]
                
                for col in columns_total.columns :
                    crosstab_result.loc[total_label, col] = columns_total.loc[total_label, col]
                
                crosstab_result.fillna(0, inplace=True)

                for idx in index_total.index :
                    crosstab_result.loc[idx, total_label] = index_total.loc[idx, total_label]
                

    else :
        if qtype in ['number', 'float'] :
            crosstab_result = calc_number(index, default_calc)
            return crosstab_result
        else :
            if isinstance(index, list) :
                crosstab_result = index_binary_total(index)

            else :
                crosstab_result = pd.crosstab(
                                    df[index],
                                    columns='count',
                                    margins=True,
                                    margins_name=total_label,
                                )

        crosstab_result = crosstab_result.loc[:, total_label].to_frame()
    
    crosstab_result = crosstab_result[[total_label] + [col for col in crosstab_result.columns if col!= total_label]]
    # print(f"Default Crosstab : {time.time() - start_time}")
    
    base_index = crosstab_result.index

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

        score_result = crosstab_result.loc[scores, :]
        score_result.sort_index(ascending=reverse_rating, inplace=True)
        base_index = score_result.index.to_list()
        
        total_df = pd.DataFrame(crosstab_result.loc[total_label, :]).T
        crosstab_result = pd.concat([total_df, score_result])
        
    
    # print(f"Total Setting : {time.time() - start_time}")

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

    # print(f"Top/Bot Setting : {time.time() - start_time}")
    
    index_order = [all_label] + [i for i in crosstab_result.index.to_list() if not i == all_label]
    column_order = [total_label] + [i for i in crosstab_result.columns.to_list() if not i == total_label]
    
    crosstab_result = crosstab_result.reindex(index_order)
    crosstab_result = crosstab_result[column_order]

    if not qtype in ['number', 'float'] :
        crosstab_result.fillna(0, inplace=True)
        crosstab_result = crosstab_result.astype(int)

    # # Calc
    # if not qtype in ['number', 'float'] and aggfunc is not None :
    #     calc_result = calc_number(index, aggfunc)
    #     calc_result = calc_result.loc[aggfunc]    
    #     if columns is not None :
    #         agg[index] = default_calc + ['count']
    #         cross_calc = pd.pivot_table(
    #                                 df,
    #                                 columns=columns,
    #                                 values=index,
    #                                 aggfunc=agg
    #                             )
    #         if isinstance(columns, list) :
    #             filt_columns = filt_var(cross_calc.columns)
    #             cross_calc = cross_calc.loc[:, filt_columns]
                
    #             cross_calc.columns = set_var(cross_calc.columns)
    #             cross_calc = cross_calc.loc[aggfunc]
            
    #         for idx in calc_result.index :
    #             cross_calc.loc[idx, total_label] = calc_result.loc[idx, total_label]

    #         calc_result = cross_calc
        
    #     crosstab_result = pd.concat([crosstab_result, calc_result])
            

    # Process index metadata
    if index_meta :
        if aggfunc is not None :
            back_index = back_index + aggfunc
        
        index_order, index_labels = extract_order_and_labels(index_meta, [all_label], back_index)
        crosstab_result = add_missing_indices(crosstab_result, index_order)
        crosstab_result = reorder_and_relabel(crosstab_result, index_order, index_labels, axis=0, name=None)

    # Process columns metadata
    if columns_meta:
        columns_order, columns_labels = extract_order_and_labels(columns_meta, [total_label])
        crosstab_result = add_missing_indices(crosstab_result.T, columns_order).T
        crosstab_result = reorder_and_relabel(crosstab_result, columns_order, columns_labels, axis=1, name=None)
    
    # print(f"Meta Setting : {time.time() - start_time}")

    if qtype in ['number', 'float'] : 
        crosstab_result = crosstab_result.loc[[all_label, *aggfunc], :]

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
        
        if (isinstance(table_type, str) and table_type in ['number', 'float', 'text']) or (isinstance(table_type, list) and any(t in ['number', 'float', 'text'] for t in table_type)) :
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
        
        if qtype in ['number', 'float', 'text'] :
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
            label_list = [{v['label']: {'vgroup': v['vgroup'], 'rowTitle': clean_text(v['rowTitle']), 'colTitle': clean_text(v['colTitle'])}} for v in variables if v['type'] in ['single', 'multiple', 'number', 'float']]

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
    