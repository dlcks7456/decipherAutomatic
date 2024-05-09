from PIL import Image
import os
import re
from IPython.display import display, HTML
import shutil
import pandas as pd
import numpy as np

def list_directories(path=None):
    '''### 현재 디렉토리의 하위 디렉토리 목록을 반환
- `path` : 디렉토리의 경로 (기본값 : None = 현재 작업 디렉토리)
    '''
    if path is None:
        path = os.getcwd()
    
    directories = [item for item in os.listdir(path) if os.path.isdir(os.path.join(path, item))]
    return directories


def list_image_files(path=None):
    '''### 지정된 경로 또는 현재 경로에서 모든 이미지 파일을 리스트로 반환
    - `.jpg`, `.jpeg`, `.png`, `.gif`, `.bmp`, `.jfif'`
    - `path`: 이미지 파일을 검색할 디렉토리 경로 (기본값: None, 현재 작업 디렉토리 사용)
    '''
    # path가 None이면 현재 작업 디렉토리를 사용합니다.
    if path is None:
        path = os.getcwd()

    # 지원되는 이미지 확장자 목록
    supported_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.jfif')
    
    # 해당 경로에서 모든 파일을 검색하고, 이미지 확장자를 가진 파일만 필터링합니다.
    image_files = [os.path.join(path, file) for file in os.listdir(path)
                   if os.path.isfile(os.path.join(path, file)) and file.lower().endswith(supported_extensions)]
    
    return image_files


def image_rename(image_path, save_folder=None, name_format="%s", file_format='png', display_print=True):
    ''' 이미지 파일 이름을 코드로 변환
    - `image_path`: 이미지 경로 및 이름
    - `save_folder`: 저장할 폴더 이름 (기본값: None)
    - `name_format`: 저장할 파일 이름 형식 (기본값: "%s") / %s 부분이 변환된 파일 이름으로 대체
    - `file_format`: 저장할 이미지 확장자 (기본값: png)
    - `display_print`: 파일 이름 변환 결과 출력 여부 (기본값: True)

    파일명에 숫자가 포함되어 있는 경우에만 작동
    파일명에 숫자가 포함되어 있지 않은 경우, 파일명을 `[NONE_CODE_이미지명]`으로 변경
    포함된 숫자가 2개 이상인 경우 '_'로 구분되어서 파일명이 변경됨 (예: 1_2.png)
    이미지 확장자는 `png`(기본값)으로 변경됨
    '''
    # 경로에서 파일 이름을 분리
    image_name = os.path.split(image_path)[-1]

    if not os.path.exists(image_path):
        if display_print :
            display(HTML(f"""❓ <b style="color: #e7046f"><i>The file does not exist</i></b> : {image_path}"""))

        return {'original_name': image_name, 'new_name': None, 'none_check': None, 'duplicate_check': None}
    
    dir_path = os.path.split(image_path)[:-1]

    file_name, image_format = os.path.splitext(image_name)
    
    # 파일명에서 숫자 추출
    find_numbers = re.findall(r'\d+', file_name)
    find_numbers = [str(int(x)) for x in find_numbers]
    new_name = None

    # 숫자가 없는 경우
    none_check = False

    if len(find_numbers) == 0:
        new_name = f'NONE_CODE_{image_name}'
        none_check = True
    else:
        new_name = '_'.join(find_numbers) + f'.{file_format}'

    new_name = name_format%new_name

    version = 1    
    # save_folder가 제공된 경우 폴더가 없으면 생성
    if save_folder is not None:
        check_dir = os.path.join(*dir_path, save_folder)
        if not os.path.exists(check_dir):
            os.makedirs(os.path.join(*dir_path, save_folder))
        # 새로운 파일 경로 조합
        new_path = os.path.join(*dir_path, save_folder, new_name)

        while True :
            if os.path.exists(new_path) :
                version += 1
                new_name = new_name.split('.')[0]
                new_name = f'{new_name}_v{version}.{file_format}'
                new_path = os.path.join(*dir_path, save_folder, new_name)
            else :
                break
        
        new_path = os.path.join(*dir_path, save_folder, new_name)

        # 파일 복사
        shutil.copy(image_path, new_path)
    else:
        # save_folder가 제공되지 않은 경우, 현재 위치에 파일 이름 변경

        while True :
            if os.path.exists(os.path.join(*dir_path, new_name)) :
                version += 1
                new_name = new_name.split('.')[0]
                new_name = f'{new_name}_v{version}.{file_format}'
            else :
                break
        
        os.rename(os.path.join(*dir_path, image_name), os.path.join(*dir_path, new_name))
    
    if len(find_numbers) >= 2 :
        if display_print :
            display(HTML(f"""⚠️ <b style="color: #e7046f">{image_name}</b> : <i>More than one number found in the regular expression</i>"""))

    if display_print : 
        display(HTML(f"""✔️ <b><i>Rename</i></b> : {image_name} → <b style="color: #2d6df6">{new_name}</b>"""))

    # 파일명 중복으로 인한 처리
    dup_check = True if version > 1 else False

    return {'original_name': image_name, 'new_name': new_name, 'none_check': none_check, 'duplicate_check': dup_check}



def image_resize(image_path, width, height, save_folder=None, display_print=True):
    ''' 이미지를 캔버스에 맞게 크기를 조정하고 투명 배경의 PNG로 저장하는 함수
    - 기본적으로 `png`로 저장되며, 원본 이미지를 제외한 배경은 투명이 된다.
    - `image_path`: 이미지 파일 경로
    - `width`: 이미지의 width
    - `height`: 이미지의 height
    - `save_folder`: 이미지 저장 폴더 (기본값: None) / None이면 원본 파일 위치에 저장
    - `display_print`: 변환 결과 출력 여부 (기본값: True)
    '''
    # 파일 존재 여부 확인
    if not os.path.exists(image_path):
        if display_print:
            display(HTML(f"""❓ <b style="color: #e7046f"><i>The file does not exist</i></b>: <span>{image_path}</span>"""))
        
        return {'image_name': os.path.basename(image_path), 'resize_image_name': None, 'original_width': None, 'new_width': None, 'original_height': None, 'new_height': None}
        
    # 이미지 로드
    img = Image.open(image_path).convert('RGBA')  # 이미지를 RGBA 모드로 변환
    img_width, img_height = img.size

    # 캔버스의 비율과 이미지의 비율을 계산
    canvas_ratio = width / height
    image_ratio = img_width / img_height

    # 이미지가 캔버스에 최대한 크게 들어가도록 크기를 조정
    if image_ratio > canvas_ratio:
        # 이미지의 가로가 캔버스의 가로에 비해 크면 가로 기준으로 조정
        new_width = width
        new_height = int(new_width / image_ratio)
    else:
        # 이미지의 세로가 캔버스의 세로에 비해 크거나 같으면 세로 기준으로 조정
        new_height = height
        new_width = int(new_height * image_ratio)

    # 이미지 크기를 조정
    resized_img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)

    # 새 캔버스 생성 (RGBA 모드, 투명 배경)
    canvas = Image.new('RGBA', (width, height), (255, 255, 255, 0))

    # 이미지를 캔버스 중앙에 배치하기 위해 시작 좌표 계산
    start_x = (width - new_width) // 2
    start_y = (height - new_height) // 2

    # 캔버스에 이미지 삽입
    canvas.paste(resized_img, (start_x, start_y), resized_img)  # resized_img를 마스크로 사용하여 투명 배경 유지

    # 결과 이미지 파일명 생성 (원본 파일명 사용)
    orignal_filename = os.path.basename(image_path)
    if save_folder is None:
        save_folder = os.path.dirname(image_path)
        new_filename = os.path.splitext(orignal_filename)[0] + '.png'
    else:
        # 지정된 폴더가 존재하지 않는 경우, 폴더 생성
        original_path = os.path.split(image_path)[:-1]
        os.makedirs(os.path.join(*original_path, save_folder), exist_ok=True)
        
        save_folder = os.path.join(*original_path, save_folder)
        new_filename = os.path.splitext(orignal_filename)[0] + '.png'

    # 최종 저장 경로
    save_path = os.path.join(*os.path.split(save_folder), new_filename)

    # 파일 저장
    canvas.save(save_path)
    if display_print :
        display(HTML(f"""✔️ <b><i>Resize and save file complete ({width}x{height})</i> : <b style="color: #2d6df6;">{save_path}</b></b>"""))

    return {'image_name': os.path.basename(image_path), 'resize_image_name': new_filename, 'original_width': img_width, 'new_width': width, 'original_height': img_height, 'new_height': height}


def get_mean_width(path=None) :
    '''### 경로에 있는 이미지의 평균 너비를 반환
- `path` : 경로 (기본값 : None) / None인 경우 현재 경로에서 진행
    '''
    images = list_image_files(path)
    if not images :
        display(HTML('''⚠️ <b style="color: #e7046f;"><i>No images found</i></b>'''))
        return
    widths = [Image.open(img).convert('RGBA').size[0] for img in images]
    return int(np.mean(widths))


def image_re_all(image_path=None, save_folder=None, name_format="%s", file_format='png', width=500, height=500, display_print=True) :
    """### `image_rename` / `image_resize` 모두 실행
- `image_path` : 이미지 경로 (기본값 : None)
- `save_folder` : 저장 경로 (기본값 : None)
- `name_format`: 저장할 파일 이름 형식 (기본값: "%s") / %s 부분이 변환된 파일 이름으로 대체
- `file_format` : 저장 파일 포맷 (기본값 : 'png')
- `width` : `resize` 시 너비 (기본값 : 500)
- `height` : `resize` 시 높이 (기본값 : 500)
- `display_print` : 변환 결과 출력 여부 (기본값 : True)
    """
    data = []

    if image_path is None :
        image_path = os.getcwd()

    parent_path = os.path.split(image_path)
    for img in list_image_files(image_path) :
        rename_data = image_rename(img, save_folder, name_format, file_format, display_print=False)
        new_name = rename_data['new_name']
        if save_folder is None :
            rename_path = os.path.join(*parent_path, new_name)
        else :
            rename_path = os.path.join(*parent_path, save_folder, new_name)
            
        resize_data = image_resize(image_path=rename_path, width=width, height=height, save_folder=None, display_print=False)
        
        data.append({**rename_data, **resize_data})
    
    if display_print :
        for d in data :
            orginal_name = d['original_name']
            new_name = d['new_name']
            none_check = d['none_check']
            duplicate_check = d['duplicate_check']

            if none_check :
                display(HTML(f"""⚠️ <b style="color: #e7046f">{orginal_name}</b> : <i>The code (number) does not exist in the filename.</i>"""))

            if duplicate_check :
                display(HTML(f"""⚠️ <b style="color: #e7046f">{orginal_name}</b> : <i>The code (number) is duplicated.</i> > <b style="color: #2d6df6;">{new_name}</b>"""))

            display(HTML(f"""✔️ <b><i>Rename/Resize Complete (Size: {width}x{height})</i> : {orginal_name} → <b style="color: #2d6df6">{new_name}</b>"""))

    return pd.DataFrame(data)




