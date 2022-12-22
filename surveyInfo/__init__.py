from decipher.beacon import api
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from ..key import api_key, api_server

def return_ymd(datevar) :
    year = datevar.year
    month = datevar.month
    day = datevar.day
    return f'{year}-{month}-{day}'

def get_surveys(
    search=None,
    favorite=False,
    status=[],
    start_date=None,
    end_date=None,
    key=api_key,
    server=api_server,
    info_all=False) :

    # API LOGIN
    try :
        api.login(key, server)
    except :
        print('❌ [ERROR] : Decipher api login failed')
        return
    
    now = datetime.now()
    defualt_date = now + relativedelta(months=-3)

    # defualt start and end
    start_date_filt = f'start_date:{return_ymd(defualt_date)}'
    end_date_filt = f'end_date:{return_ymd(now)}'

    if start_date :
        try :
            chk_start_date = datetime.strptime(start_date, '%Y-%m-%d')
        except :
            print('❌ [ERROR] : start_date format is \'%Y-%m-%d\'')
            return
        start_date_filt = f'start_date:{return_ymd(chk_start_date)}'
    if end_date :
        try :
            chk_end_date = datetime.strptime(end_date, '%Y-%m-%d')
        except :
            print('❌ [ERROR] : end_date format is \'%Y-%m-%d\'')
            return
        end_date_filt = f'end_date:{return_ymd(chk_end_date)}'
    
    if start_date and end_date :
        if chk_start_date > chk_end_date :
            print('❌ [ERROR] : Please check date')
            return 
    
    # favorite_edit
    favorite_filt = None
    if favorite : 
        favorite_filt = 'my:favorite'

    # status
    if type(status) != list :
        print('❌ [ERROR] : The status is only list')
        return

    states = ['testing', 'live', 'closed', 'active', 'beacon', 'campaign', 'info', 'spss']
    type_flit = None
    if status :
        for s in status : 
            if not s in states :
                print(f'❌ [ERROR] : {s} Please check status (\'testing\', \'live\', \'closed\', \'active\', \'beacon\', \'campaign\', \'info\', \'spss\')')
                return
        types = ','.join(status)
        type_flit = f'type:({types})'

    queory_filt = [search, favorite_filt, type_flit, start_date_filt, end_date_filt]
    quoery = [q for q in queory_filt if q != None]
    quoery = ' '.join(quoery)

    surveys = api.get(f'/rh/companies/all/surveys', query=quoery)

    df_surveys = pd.DataFrame(surveys)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    chk_index = list(df_surveys.index)
    
    if chk_index :
        df_surveys['pid'] = df_surveys['path'].apply(lambda x : x.split('/')[-1])
        df_surveys['creator'] = df_surveys['createdBy'].apply(lambda x : x['email'])
        if info_all :
            return df_surveys
        else :
            show_columns = ['pid', 'title', 'state', 'creator', 'createdOn']
            return df_surveys[show_columns]
    else :
        print('❓ No projects could be found with the query')
        print(f'❗ quoery : {quoery}')
