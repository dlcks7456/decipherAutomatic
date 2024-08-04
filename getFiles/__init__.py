from decipher.beacon import api
import os
import time
from datetime import datetime
import pandas as pd
from ..key import api_key, api_server
from pandas.io.formats import excel

def chk_mkdir(path_name) :
    if not os.path.exists(path_name) :
        os.mkdir(path_name)

def create_binary_file(path, file_name, file) :
    file_path = os.path.join(path, file_name)
    with open(file_path, 'wb') as f: 
        f.write(file)

def create_ascii_file(path, binary_file) :
    get_binary_file_path = os.path.join(path, binary_file)
    if '.csv' in binary_file :
        csv = pd.read_csv(os.path.join(path, binary_file), low_memory=False)
        csv.to_csv(f'{path}/{binary_file}', index=False, encoding='utf-8-sig')

    else :
        with open(f'{path}/{binary_file}', 'w', encoding='utf-8-sig') as f :
            get_binary_file = open(get_binary_file_path, 'r', encoding='utf-8-sig')
            for line in get_binary_file.readlines() :
                f.write(line)
            get_binary_file.close()

    # os.remove(get_binary_file_path)

def project_files(
    pid,
    key = api_key,
    server = api_server,
    mkdir=False,
    dir_name=None,
    delivery=False,
    cond='',
    xml=False,
    data=False,
    layout=False,
    quota=False,
    lang=False,
    ce='input',
    oe='input') :

    #pd.io.formats.excel.ExcelFormatter.header_style = None
    excel.ExcelFormatter.header_style = None
    
    if pid == None or pid == '' :
        print('❌ [ERROR] : pid is blank')
        return

    path = f'surveys/selfserve/548/{pid}'

    try :
        api.login(key, server)
    except :
        print('❌ [ERROR] : Decipher api login failed')
        return 
    
    # folder create check
    parent_path = os.getcwd()
    if mkdir :
        folder_name = pid
        if dir_name != None :
            folder_name = dir_name
        parent_path = os.path.join(parent_path, folder_name)
        chk_mkdir(parent_path)

    # paths 
    delivery_path = os.path.join(parent_path, 'Delivery data')
    data_path = os.path.join(parent_path, 'All data')
    layout_path = os.path.join(parent_path, 'Layouts')
    file_path = os.path.join(parent_path, 'Survey files')
    lang_path = os.path.join(parent_path, 'Languages')

    # Delivey data download
    if delivery :
        if cond == None or cond.isdigit() :
            print('❌ [ERROR] : The cond argument can only be a string')
            return
        delivery_cond = 'qualified' if cond == '' else f'qualified and {cond}' 

        chk_mkdir(delivery_path)

        now = datetime.now()
        now_year = now.year
        now_month = '{0:02}'.format(now.month)
        now_day = '{0:02}'.format(now.day)
        date_dir = f'{now_year}{now_month}{now_day}'

        original_path = os.getcwd()
        delivery_date_path = os.path.join(delivery_path, date_dir)
        chk_mkdir(delivery_date_path)

        info = api.get(f'rh/{path}')
        title_name = info['title']
        layouts = api.get(f'{path}/layouts')
        datamap = api.get(f'{path}/datamap', format='json')
        time.sleep(3)
        layout_summary = [(layout['description'], layout['id']) for layout in layouts if layout['id']]
        layout_ids = [layout['id'] for layout in layouts if layout['id']]

        layout_msg = [f'  📃 {id} ({name})' for name, id in layout_summary]

        print(f'🏁 Start delivery data download (pid = {pid})')
        print(' 📣 Enter layout ID')
        layout_msg = '\n'.join(layout_msg)
        print(layout_msg)
        print('')
        time.sleep(2)
        if layout_ids :
            str_id = [str(each) for each in layout_ids]
            if (not ce in ['input', 'standard']) and (not str(ce) in str_id) :
                print('❌ [ERROR] : The CE Layout ID does not exist.')
                return
            
            if (not oe in ['input', 'standard']) and (not str(oe) in str_id) :
                print('❌ [ERROR] : The OE Layout ID does not exist.')
                return

            layout_ids
            print(' ❗ If you want to use the Standard layout, press the ESC button')
            print(' 📊 Enter the CE data layout')
            ce_layout_id = None
            if ce == 'input' :
                while True :
                    ce_layout_id = input('📊 CE data layout id : ')
                    if ce_layout_id == '' :
                        break
                    elif not ce_layout_id.isdigit() :
                        print('  ❌ [ERROR] : The CE data layout id is only number')
                    elif not int(ce_layout_id) in layout_ids :
                        print('  ❌ [ERROR] : Please check the layout id')
                    else :
                        break
            elif ce == 'standard' :
                ce_layout_id = None
            else :
                ce_layout_id = str(ce)

            if ce_layout_id == '' :
                ce_layout_id = None
                print(' 🔔 CE data layout : Standard')
            else :
                print(f' 🔔 CE data layout : ID = {ce_layout_id}')

            time.sleep(1)
            print('')

            print(' 📝 Enter the OE data layout')
            oe_layout_id = None
            if oe == 'input' :
                while True :
                    oe_layout_id = input('📝 OE data layout id : ')
                    if oe_layout_id == '' :
                        break
                    if not oe_layout_id.isdigit() :
                        print('  ❌ [ERROR] : The OE data layout id is only number')
                    elif not int(oe_layout_id) in layout_ids :
                        print('  ❌ [ERROR] : Please check the layout id')
                    else :
                        break
            elif oe == 'standard' :
                oe_layout_id = None
            else :
                oe_layout_id = str(oe)

            if oe_layout_id == '' :
                oe_layout_id = None
                print(' 🔔 OE data layout : Standard')
            else :
                print(f' 🔔 OE data layout : ID = {oe_layout_id}')    
        else :
            print(' ❓ Data layout is null, the layout setting to standard')
            ce_layout_id = None
            oe_layout_id = None
        print('')

        # CE data (fw, sav)
        print(f' 📊 CE data downloading... ⌛')
        try :
            if ce_layout_id :
                ce_layout_id = int(ce_layout_id)
                spss_fw = api.get(f'{path}/data', format='spss_data', layout=ce_layout_id, cond=delivery_cond)
                time.sleep(3)
                spss_sav = api.get(f'{path}/data', format='spss16', layout=ce_layout_id, cond=delivery_cond)
            else :
                spss_fw = api.get(f'{path}/data', format='spss_data', cond=delivery_cond)
                time.sleep(3)
                spss_sav = api.get(f'{path}/data', format='spss16', cond=delivery_cond)
        except :
            print(' ❌ [ERROR] : Get Data download API is Error - CE / 🆖 Hint : Please check width of layout')


        time.sleep(3)
        fw_zip_name = f'fixed-width.zip'
        create_binary_file(delivery_date_path, fw_zip_name, spss_fw)
        
        sav_zip_name = f'sav.zip'
        create_binary_file(delivery_date_path, sav_zip_name, spss_sav)

        print(' 🔔 The CE data download is done')
        print('')

        # OE data
        print(' 📝 OE data downloading... ⌛')

        try :
            panner_key = ['GID', 'sname', 'uid', 'sid', 'eid', 'gid', 'GUID', 'pid', 'psid']
            system_vars = ['date', 'markers', 'vlist', 'qtime', 'vos', 'vosr15oe', 'vbrowser', 'vbrowserr15oe', 'vmobiledevice', 'vmobileos', 'start_date', 'vdropout', 'source', 'decLang', 'userAgent', 'dcua', 'url', 'session', 'ipAddress', 'qtime', 'HQTolunaEnc', 'Feedback', 'feedback']
            # diff_vars = panner_key + system_vars
            diff_vars = []
            txt_vars = [var['label'] for var in datamap['variables'] if var['type'] == 'text' and not var['label'] in diff_vars]

            oe_fileds = ','.join(txt_vars)

            if oe_layout_id :
                ce_layout_id = int(ce_layout_id)
                oe_data = api.get(f'{path}/data', format='csv', layout=oe_layout_id, cond=delivery_cond)
            else :
                oe_data = api.get(f'{path}/data', format='csv', fields=oe_fileds, cond=delivery_cond)

            time.sleep(3)

            binary_csv_name = f'OE.csv'
            create_binary_file(delivery_date_path, binary_csv_name, oe_data)
            create_ascii_file(delivery_date_path, binary_csv_name)
            
            print(' 🔔 The OE data download is done')
        except :
            print(' ❌ [ERROR] : Get Data download API is Error - OE')
        print('')

        # Excel all data
        print(' 🧩 Excel all data downloading... ⌛')
        try :
            excel_data = api.get(f'{path}/data', format='csv', cond=delivery_cond)

            time.sleep(3)
            
            binary_excel_name = f'ALL.csv'
            create_binary_file(delivery_date_path, binary_excel_name, excel_data)
            create_ascii_file(delivery_date_path, binary_excel_name)

            print(' 🔔 The excel data download is done')
        except :
            print(' ❌ [ERROR] : Get Data download API is Error - Excel data')
        print('')

        # Map xlsx download
        try :
            print(' 🌏 Data map(xlsx) downloading... ⌛')
            map_xlsx = api.get(f'{path}/datamap', format='xlsx')
            create_binary_file(delivery_date_path, f'map.xlsx', map_xlsx)

            time.sleep(3)

            print(' 🔔 The data map(xlsx) download is done')
        except :
            print(' ❌ [ERROR] : Get Data download API is Error - data map file (xlsx)')
        print('')

        print('✅ Delivery data download is done ✅')
        print('')
        print('')

    if any([xml, data, layout, quota, lang]) :
        print('🏁 Start Decipher project backup')
    # Data backup
    if data :
        print(' 📥 Data BackUp ... ⌛')
        chk_mkdir(data_path)
        try :
            csv_data = api.get(f'{path}/data', format='csv', cond='everything')

            binary_csv_name = f'{pid}.csv'
            create_binary_file(data_path, binary_csv_name, csv_data)
            create_ascii_file(data_path, binary_csv_name)

            time.sleep(3)

            tab_data = api.get(f'{path}/data', format='tab', cond='everything')

            binary_txt_name = f'{pid}.txt'
            create_binary_file(data_path, binary_txt_name, tab_data)
            create_ascii_file(data_path, binary_txt_name)

            time.sleep(3)
            print(' 🔔 Data BackUp is done')
        except :
            print(' ❌ [ERROR] : Get Data API is Error')
        print('')

    # Layout backup
    if layout :
        print(' 🦺 Layout BackUp ... ⌛')
        try :
            layouts = api.get(f'{path}/layouts')

            if layouts :
                chk_mkdir(layout_path)
                with open(os.path.join(layout_path, 'layouts.py'), 'w', encoding='utf-8') as f :
                    f.write(f'layout = {layouts}')

                for layout in layouts :
                    layout_name = layout['description']
                    layout_variables = layout['variables']
                    
                    with open(os.path.join(layout_path, f'{layout_name}.txt'), 'w', encoding='utf-8') as f :
                        for variable in layout_variables :
                            label = variable['label']
                            fwidth = variable['fwidth']
                            altlabel = variable['altlabel']
                            shown = variable['shown']
                            if shown :
                                f.write(f'{label},{altlabel},{fwidth}\n')
                print(' 🔔 Layout BackUp is done')
                time.sleep(2)
            else :
                print(' ❗ Layout is null')
        except :
            print(' ❌ [ERROR] : Get Layout API is Error')
        print('')

    # XML backup
    if xml :
        print(' 📋 XML BackUp ... ⌛')
        chk_mkdir(file_path)
        survey_xml = 'survey.xml'
        try :
            get_survey_xml = api.get(f'{path}/files/{survey_xml}')
            create_binary_file(file_path, survey_xml, get_survey_xml)
            time.sleep(2)
            print(' 🔔 XML BackUp is done')
        except :
            print(' ❌ [ERROR] : Get XML API is Error')
        
        print('')

    # Quota backup
    if quota :
        print(' 💻 Quota BackUp ... ⌛')
        chk_mkdir(file_path)
        try :
            quota_xls = 'quota.xls'
            get_quota = api.get(f'{path}/files/{quota_xls}')
            create_binary_file(file_path, quota_xls, get_quota)
            time.sleep(2)
            print(' 🔔 Quota BackUp is done')
        except :
            print(' ❌ [ERROR] : Get quota API is Error')
        print('')

    # Languages backup
    if lang :
        print(' 🛫 Language BackUp ... ⌛')
        try :
            language_manager_data = api.get(f'{path}/mls/application/data')

            survey_langs = language_manager_data['SURVEY_LANGUAGE_LIST']
            primary_lang = language_manager_data['PRIMARY_LANGUAGE']
            survey_langs.remove(primary_lang)

            if survey_langs :
                chk_mkdir(lang_path)
                for lang in survey_langs :
                    lang_xml = f'{lang}.xml'
                    try : 
                        get_lang_xml = api.get(f'{path}/files/{lang_xml}')
                    except :
                        print(f'❗ the {lang} is not exist')
                    create_binary_file(lang_path, lang_xml, get_lang_xml)
                    time.sleep(2)
                print(' 🔔 Language BackUp is done')
            else :
                print(' ❗ Language is null')
        except :
            print(' ❌ [ERROR] : Get Language API is Error')
        print('')

    print('✅ BackUp is done ✅')
    print('')


def get_layout_id(
        pid,
        key = api_key,
        server = api_server,
    ) :
    #pd.io.formats.excel.ExcelFormatter.header_style = None
    excel.ExcelFormatter.header_style = None
    
    if pid == None or pid == '' :
        print('❌ [ERROR] : pid is blank')
        return

    path = f'surveys/selfserve/548/{pid}'

    try :
        api.login(key, server)
    except :
        print('❌ [ERROR] : Decipher api login failed')
        return     

    layouts = api.get(f'{path}/layouts')
    datamap = api.get(f'{path}/datamap', format='json')
    
    layout_summary = [(layout['description'], layout['id']) for layout in layouts if layout['id']]
    layout_dict = {id: name for name, id in layout_summary}

    return layout_dict

