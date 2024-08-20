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
from collections import OrderedDict, defaultdict, Counter
import json
from decipher.beacon import api
import time
from decipherAutomatic.key import api_key, api_server
from decipherAutomatic.getFiles import *
from decipherAutomatic.utils import *
from pandas.io.formats import excel
import zipfile
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from langchain.chat_models import ChatOllama
from langchain_openai import ChatOpenAI
from langchain_experimental.llms.ollama_functions import OllamaFunctions
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.schema.runnable import RunnablePassthrough
from wordcloud import WordCloud
import nltk
from nltk.corpus import stopwords
from konlpy.tag import Okt

def single_total(data, base, total_label='Total') :
    if len(data[~data[base].isna()]) == 0 :
        sa = pd.DataFrame([0], index=[total_label], columns=[total_label])
    else :
        sa = pd.crosstab(
                data[base],
                columns='count',
                margins=True,
                margins_name=total_label,
            )
    
    return sa.loc[:, total_label].to_frame()

def multiple_total(data, base, total_label='Total'):
    tabs = []
    if data.empty:
        for row in base:
            zero_row = pd.DataFrame([0], index=[row], columns=[total_label])
            tabs.append(zero_row)
    else:
        for row in base:
            ma = pd.crosstab(data[row], columns='count', margins=True, margins_name=total_label)
            if not ma.empty:
                chk = ma.index.difference([total_label]).tolist()
                if chk:
                    rename_dict = {chk[0]: row}
                    ma.rename(index=rename_dict, inplace=True)
                    tabs.append(ma[[total_label]])

    if not tabs:
        zero_row = pd.DataFrame([0], index=base, columns=[total_label])
        tabs.append(zero_row)

    ma_table = pd.concat(tabs)
    cdf = data[base].copy()
    cdf['cnt'] = (cdf != 0).sum(axis=1)
    ma_table.loc[total_label, total_label] = (cdf['cnt'] > 0).sum()

    return ma_table.loc[~ma_table.index.duplicated(), :]

def number_total(data, cols, aggfunc, total_label='Total') :
    default_number = data[cols].describe().to_frame()

    aggfunc = ['50%' if func == 'median' else func for func in aggfunc]

    default_number = default_number.loc[aggfunc]
    default_number.rename(index={'count': total_label, '50%': 'median'}, inplace=True)
    default_number.columns = pd.Index([total_label])

    default_number.fillna(0, inplace=True)

    return default_number


def number_with_columns(df, index, columns, aggfunc, total_label='Total'):
    if columns is None:
        raise ValueError('columns cannot be None')

    if index is None:
        raise ValueError('index cannot be None')

    if isinstance(columns, list):
        # 첫 번째 crosstab 생성
        crosstab = number_total(df, index, aggfunc)

        for col in columns:
            cond = (~df[col].isna())
            filtered_df = df[cond]
            if not filtered_df.empty:
                nb = number_total(filtered_df, index, aggfunc)
                nb.columns = [col]
                nb.fillna(0, inplace=True)
                crosstab[col] = nb[col]
            else:
                crosstab[col] = 0
    else:
        pivot_dict = {index: aggfunc}
        
        # 첫 번째 crosstab 생성
        crosstab = number_total(df, index, aggfunc)
        crosstab.rename(index={'count': total_label}, inplace=True)

        # Pivot table 생성
        number_cols = pd.pivot_table(df, 
                                     columns=columns, 
                                     values=index, 
                                     aggfunc=pivot_dict)
        
        number_cols.rename(index={'count': total_label}, inplace=True)

        for col in number_cols.columns:
            crosstab[col] = number_cols[col]

    return crosstab


def create_crosstab(df: pd.DataFrame,
                    index: Union[str, List[str]],
                    columns: Optional[Union[str, List[str]]] = None,
                    total_label: str = 'Total') -> pd.DataFrame:
    """
    Creates a crosstab from the provided DataFrame with optional metadata for reordering and relabeling indices and columns, and with options to include top/bottom summaries and index sorting.
    
    Parameters:
        df (pd.DataFrame): The input DataFrame.
        index (str or list): The column name or list of column names to use for the crosstab index.
        columns (str or list, optional): The column name or list of column names to use for the crosstab columns.
    
    Returns:
        pd.DataFrame: The resulting crosstab with optional reordering, relabeling, top/bottom summaries, and total sum row.
    """

    def rename_total_dict(col):
        return {total_label: col}

    if not isinstance(index, (str, list)):
        raise ValueError("Index must be either a string or a list of strings.")
    
    if columns is not None:
        if not isinstance(columns, (str, list)):
            raise ValueError("Columns must be either a string or a list of strings.")

        # SA BY MA or MA BY SA
        if (isinstance(index, str) and isinstance(columns, list)) or (isinstance(index, list) and isinstance(columns, str)):
            base_row = index if isinstance(index, str) else columns
            base_col = columns if isinstance(index, str) else index
            
            index_total = single_total(df, base_row)
            sa_table = [
                single_total(df[~df[col].isna()], base_row).rename(columns=rename_total_dict(col))
                for col in base_col
            ]
            
            crosstab_result = pd.concat([index_total, *sa_table], axis=1).fillna(0)
            if isinstance(index, list) and isinstance(columns, str):
                crosstab_result = crosstab_result.T

        # MA BY MA
        elif isinstance(index, list) and isinstance(columns, list):
            index_total = multiple_total(df, index)
            ma_table = [
                multiple_total(df[~df[col].isna()], index).rename(columns=rename_total_dict(col))
                for col in columns
            ]
            crosstab_result = pd.concat([index_total, *ma_table], axis=1).fillna(0)
        
        else:
            crosstab_result = pd.crosstab(df[index], df[columns], margins=True, margins_name=total_label)
            if len(crosstab_result) == 0 :
                crosstab_result = pd.DataFrame([0], index=[total_label], columns=[total_label])
    
    else:
        if isinstance(index, list):
            crosstab_result = multiple_total(df, index)
        else:
            crosstab_result = single_total(df, index)

        crosstab_result = crosstab_result.loc[:, total_label].to_frame()
    
    return crosstab_result[[total_label] + [col for col in crosstab_result.columns if col != total_label]]



def top_setting(crosstab, top, diff_cols=None):
    if diff_cols is None:
        diff_cols = []
    diff_cols.append('Total')
    base_crosstab = crosstab.loc[~crosstab.index.isin(diff_cols), :]

    if top is None:
        return None
    
    if isinstance(top, int):
        top = [top]

    top = list(set(top))  # 중복 제거
    top_result = [
        pd.DataFrame([base_crosstab.iloc[:t].sum().rename(f'Top {t}')])
        for t in top
    ]
    
    return pd.concat(top_result)


def medium_setting(crosstab, medium, diff_cols=None):
    if diff_cols is None:
        diff_cols = []
    diff_cols.append('Total')
    base_crosstab = crosstab.loc[~crosstab.index.isin(diff_cols), :]

    if medium is None:
        return None
    
    if isinstance(medium, int):
        medium = [medium]

    medium = list(set(medium))  # 중복 제거
    filt_index = [idx for idx in base_crosstab.index if idx in medium]
    medium_indices = base_crosstab.loc[filt_index].sum()
    
    medium_txt = ', '.join(map(str, medium))
    medium_indices.name = f'Medium ({medium_txt})'
    
    return pd.DataFrame([medium_indices])


def bottom_setting(crosstab, bottom, diff_cols=None):
    if diff_cols is None:
        diff_cols = []
    diff_cols.append('Total')
    base_crosstab = crosstab.loc[~crosstab.index.isin(diff_cols), :]

    if bottom is None:
        return None
    
    if isinstance(bottom, int):
        bottom = [bottom]

    bottom = list(set(bottom))  # 중복 제거
    bottom_result = [
        pd.DataFrame([base_crosstab.iloc[-b:].sum().rename(f'Bottom {b}')])
        for b in bottom
    ]
    
    return pd.concat(bottom_result)


def var_netting(crosstab, net) :
    net_result = []
    for name, code in net.items():
        if any(not c in crosstab.index for c in code) :
            raise ValueError(f'The code({code}) does not exist.')
        base_crosstab = crosstab.loc[code, :]
        net_result.append(pd.DataFrame([base_crosstab.sum().rename(name)]))
    
    return pd.concat(net_result)


def rating_netting(rating_crosstab_result, 
                   scores, 
                   reverse_rating=False, 
                   total_label='Total', 
                   top=None, 
                   bottom=None, 
                   medium=True):
    
    result = rating_crosstab_result.copy()
    
    # Ensure all scores are in the index
    missing_scores = [idx for idx in scores if idx not in result.index]
    result = pd.concat([result, pd.DataFrame(0, index=missing_scores, columns=result.columns)])
    
    score_result = result.loc[scores].sort_index(ascending=reverse_rating)
    
    if total_label not in result.index:
        result.loc[total_label] = 0
    
    total_df = result.loc[[total_label]]
    result = pd.concat([total_df, score_result])
    result.fillna(0, inplace=True)

    net_crosstab = []

    def get_unique_list(lst):
        if isinstance(lst, int):
            return [lst]
        return list(set(lst))
    
    if all([n is not None for n in [top, bottom]]) and medium:
        top_list = get_unique_list(top)
        bot_list = get_unique_list(bottom)

        vtop = min(top_list)
        vbot = min(bot_list)
        medium = [idx for idx in result.index if idx != total_label][vbot:-vtop]

    settings = {
        'top': top,
        'medium': medium,
        'bottom': bottom
    }

    for key, val in settings.items():
        if val:
            func = globals()[f'{key}_setting']
            net_result = func(result, val)
            if net_result is not None:
                net_crosstab.append(net_result.fillna(0))

    if net_crosstab and max(scores) > 3:
        net_table = pd.concat(net_crosstab)
        result = pd.concat([result, net_table])
        result = result.astype(int)

    return result


def preprocess_text(text, language='korean'):

    """
    텍스트를 전처리하는 함수
    - 특수문자 제거
    - 불용어 제거
    """
    # 특수문자 제거
    
    filtered_words = text
    try :
        if language == 'korean':
            # 한글 불용어 리스트
            korean_stopwords = set([
        '의', '가', '이', '은', '는', '들', '을', '를', '에', '와', '과', 
        '한', '하다', '있다', '되다', '수', '하다', '되다', '않다', '그', 
        '이다', '다', '에서', '와', '또한', '더', '그것', '그리고', '하지만', 
        '그러나', '어떤', '때문에', '대해', '것', '같다', '때문', '위해', 
        '무엇', '이것', '저것', '해서', '더', '또', '이것', '저것', '모두', 
        '아니', '오직', '대해', '후', '말', '만', '매우', '곧', '여기', '바로'
        ])

            # 형태소 분석기 초기화
            okt = Okt()
            # 불용어 제거 (한글)
            words = okt.morphs(text)
            filtered_words = [word for word in words if word not in korean_stopwords]
        else:
            text = re.sub(r'[^a-zA-Z\s]', '', text)
            # 형태소 분석 및 불용어 제거 (영어)
            # NLTK의 불용어 데이터를 다운로드
            nltk.download('stopwords', quiet=True)
            stop_words = set(stopwords.words('english'))
            words = text.split()
            filtered_words = [word for word in words if word.lower() not in stop_words]
    
    except Exception as e:
        # print(f"Error: {e}")
        return text
    
    return ' '.join(filtered_words)

def cloude_color_func(word, font_size, position, orientation, random_state=None, **kwargs):
    """
    빈도에 따라 색상이 진해지도록 설정하는 함수
    """
    # 빈도에 따라 색상을 조정
    base_color = 0x2d6df6
    r = (base_color >> 16) & 255
    g = (base_color >> 8) & 255
    b = base_color & 255
    
    # 폰트 사이즈에 따라 색상의 진하기를 조절
    max_font_size  = 200
    alpha = min(255, int(font_size / max_font_size * 255))
    return f"rgba({r}, {g}, {b}, {alpha})"


def create_wordcloud(data, font_path='malgun.ttf', file_name=None, image_path=None, width=800, height=500):
    """
    주어진 데이터프레임의 특정 컬럼을 기준으로 워드클라우드를 생성하고 저장하는 함수
    
    Parameters:
    dataframe (pd.DataFrame): 워드클라우드를 생성할 데이터프레임
    column_name (str): 워드클라우드를 생성할 컬럼명
    font_path (str): 한글이 가능한 폰트 경로, 기본값은 'malgun.ttf'
    image_path (str): 이미지 저장 경로, 지정되지 않으면 저장하지 않음
    
    Returns:
    WordCloud: 생성된 워드클라우드 객체
    """
    # 데이터프레임에서 해당 컬럼의 데이터 추출
    text = ' '.join(list(data.astype(str).values))
    text = preprocess_text(text)

    # 워드클라우드 생성
    wordcloud = WordCloud(
            font_path=font_path, 
            background_color='white', 
            width=width, 
            height=height, 
            color_func=cloude_color_func
        ).generate(text)
    
    # 워드클라우드 이미지 저장
    if image_path and file_name :
        image_path = os.path.join(image_path, file_name)
        wordcloud.to_file(image_path) 

    return wordcloud


def show_wordcloud(wordcloud, sub_title=None):    
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')    
    plt.axis('off')

    if sub_title is not None :
        plt.suptitle(sub_title, fontsize=14)

    plt.show()


def wordcloud_table(df, 
                    index,
                    columns=None,
                    font_path='malgun.ttf', 
                    image_path=None, 
                    width=800, 
                    height=500) :
    
    if columns is not None and not isinstance(columns, (str, list)) :
        raise ValueError('columns must be str or list')

    if columns is None :
        result = create_wordcloud(df[index],
                                  font_path=font_path, 
                                  file_name=f'{index}.png',
                                  image_path=image_path, 
                                  width=width, 
                                  height=height)
        
        return result

    # 2 Dimensional
    result_list = []
    if isinstance(columns, str) :
        base_values = df[columns].value_counts().sort_index().index.tolist()

        for base in base_values :
            filt_df = df[df[columns]==base][index]
            if len(filt_df) == 0 :
                continue
            result = create_wordcloud(filt_df, 
                                        font_path=font_path, 
                                        file_name=f'{columns}_Answer code {base}.png',
                                        image_path=image_path, 
                                        width=width, 
                                        height=height)
            result_list.append((base, result))
    
    if isinstance(columns, list) :
        for base in columns :
            filt_df = df[(~df[base].isna()) & (df[base]!=0)][index]
            if len(filt_df) == 0 :
                continue
            result = create_wordcloud(filt_df, 
                                    font_path=font_path, 
                                    file_name=f'{base}_Answer.png',
                                    image_path=image_path, 
                                    width=width, 
                                    height=height)
            
            result_list.append((base, result))

    
    return result_list

class WordCloudHandler :
    def __init__(self, question_title, cloud_list, variable=None, base_desc=None, sample_size=None) :
        self.title = question_title
        self.cloud_list = cloud_list
        self.variable = variable
        self.base_desc = base_desc
        self.sample_size = sample_size
    
    def show(self, desc=None) :
        if desc is None :
            desc = self.title
        
        if desc is not None :
            display(HTML(f"""<div style="font-size: 1rem; font-weight:bold;padding: 7px; max-width: 600px; font-style: italic; margin-bottom: 7px;">
                        {desc}
            </div>"""))
        
        if isinstance(self.cloud_list, WordCloud) :
            cloud = self.cloud_list
            show_wordcloud(cloud)
        
        if isinstance(self.cloud_list, list) :
            for cloud in self.cloud_list :
                show_wordcloud(cloud[1], sub_title=cloud[0])

class BannerWordCloud :
    def __init__(self, cloud_list, variable=None, base_desc=None, sample_size=None) :
        self.cloud_list = cloud_list
        self.variable = variable
        self.base_desc = base_desc
        self.sample_size = sample_size
    
    def show(self, desc=None) :
        for head, cloud in self.cloud_list :
            cloud.show(desc)



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

        result = self.copy()
        result = result.astype(float)
        all_value = result.iloc[0]

        mask_index = ['mean', 'man', 'min', 'max', 'median', 'std', '100 point conversion', 'Total']
        if isinstance(result.index, pd.MultiIndex) :
            mask = ~result.index.isin([idx for idx in result.index if idx[-1] in mask_index])
        else :
            mask = ~result.index.isin(mask_index)
        
        result.loc[mask, :] = (result.loc[mask, :].div(all_value)) * 100
        if ratio_round is not None :
            result.loc[mask, :] = result.loc[mask].round(ratio_round)

        if heatmap :
            cmap = LinearSegmentedColormap.from_list("custom_blue", ["#ffffff", "#2d6df6"])
            result.fillna(0, inplace=True)
            
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
    rank_flag = ['1순위', '2순위', '1st', '2nd']

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
    rank_flag = ['1순위', '2순위', '1st', '2nd']
    
    return_questions = []
    for q in questions :
        qlabel = q['qlabel']
        qtype = q['type']
        title = q['qtitle']
        variables = q['variables']
        label_list = {v['label']: {'vgroup': v['vgroup'], 'rowTitle': clean_text(v['rowTitle']), 'colTitle': clean_text(v['colTitle'])} for v in variables} 
        value_list = []
        meta_list = []
        oe_variables = []
        grouping = q['grouping']
        if not qtype in ['text'] :
            set_oe_v = {}
            oe_variables = [{'qlabel': v['label'], \
                            'type': 'other_open', \
                            'row': v['row'], \
                            'col': v['col'], \
                            'variables': {v['label']: {'vgroup': qlabel, 'rowTitle': clean_text(v['rowTitle']), 'colTitle': clean_text(v['colTitle'])}},\
                            'title': title,\
                            'grouping': grouping, \
                            'meta': v['title'],\
                            } for v in variables if v['type']=='text']
            label_list = {v['label']: {'vgroup': v['vgroup'], 'rowTitle': clean_text(v['rowTitle']), 'colTitle': clean_text(v['colTitle'])} for v in variables if v['type'] in ['single', 'multiple', 'number', 'float']}

        if 'values' in q.keys():
            values = q['values']
            value_list = [x['value'] for x in values]
            meta_list = {x['value']: x['title'] for x in values}
        else :
            value_list = [v['value'] for v in variables if 'value' in v.keys()]
            meta_list = {
                        x['label']: { \
                        'value': x['value'] if 'value' in x.keys() else None, \
                        'rowTitle': clean_text(x['rowTitle']), \
                        'colTitle': clean_text(x['colTitle'])}
                        for x in variables
                        } 
        
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
                ma_label_list = {v['label']: {'rowTitle': clean_text(v['rowTitle']), 'colTitle': clean_text(v['colTitle']), 'vgroup': v['vgroup']} for v in filt_variable}
                ma_values = [v['value'] for v in filt_variable if 'value' in v.keys()]
                ma_meta = {
                            x['label']: { \
                            'value': x['value'] if 'value' in x.keys() else None, \
                            'rowTitle': clean_text(x['rowTitle']), \
                            'colTitle': clean_text(x['colTitle'])}
                            for x in filt_variable
                          } 
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
    