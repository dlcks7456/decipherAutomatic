import os
import time
from datetime import datetime
import re
from typing import Union, List, Tuple, Dict, Optional, Literal, Callable, Any, TypedDict
from IPython.display import Markdown, display

'''
안녕하세요. {who}
SUD팀 {sp}입니다.

본 프로젝트 테스트 링크 전달드립니다.
[TEST LINK]
{test_link}

[PROJECT BOX]
{pb_link}

수정 사항은 Project box의 Survey Programming 시트에 업데이트 부탁드립니다.
수정 사항 작성하실 때 Question/Detail/Stage/Type/Change code/Source/Opened Date는 필수로 입력 부탁드립니다.
워딩 수정은 테스트 링크 하단의 edit 기능으로 수정 부탁드리며, 프로그래밍 언어는 수정되지 않도록 부탁드립니다.
'''


def write_test_mail(pid: Union[str, int, None] = None, 
                    to: Union[str, List[str], None] = None, 
                    sp: Union[str, None] = None, 
                    pb: Union[str, None] = None,
                    login: bool = False,
                    user: bool = False,
                    note_file: Union[str, None] = None,) -> str :
    # pid 검증
    if pid is None :
        raise ValueError("pid is required")
    else :
        if not isinstance(pid, (str, int)) :
            raise ValueError("pid must be str or int")
    
    
    # sp 검증 (보내는 사람 이름)
    if sp is not None and not isinstance(sp, str):
        raise ValueError("sp must be str")

    #  pb 검증 (프로젝트 박스 링크)
    if pb is not None and not isinstance(pb, str) :
        raise ValueError("pb must be str")

    # 받는 사람 설정
    to_text = f''
    if isinstance(to, list) :
        to_text = [f'{w}님' for w in to]
        to_text = ', '.join(to_text)
    else :
        if to is not None :
            to_text = f'{to}님'

    # 메일 내용 인사말 작성 부분
    head_text = f'''안녕하세요. {to_text}  
SUD팀 {sp}입니다.

'''
    
    if sp is None :
        head_text = ''
        if to is not None :
            head_text = f'''안녕하세요. {to_text}  
'''
    

    test_link  = f'https://pacific.surveys.nielseniq.com/survey/selfserve/548/{pid}?list=0&testLive=1'
    login_link = ''
    user_link  = ''

    if login or user :
        test_link  = f'''📌 **Open Link**  
[{test_link}]({test_link})  
'''

    if login :
        login_link = f'https://pacific.surveys.nielseniq.com/page/selfserve/548/{pid}/resLogin.html'
        login_link = f'''📌 **Login Link**  
[{login_link}]({login_link})  
'''

    user_link_desc = ''

    if user :
        user_link = f'https://pacific.surveys.nielseniq.com/survey/selfserve/548/{pid}?list=9&CO=KR&UID=[UID]'
        user_link = f'''📌 **User Link**  
[{user_link}]({user_link})  '''
        
        if login :
            user_link_desc = f''': 로그인 링크에서 입력한 ID로 USER LINK에 접속되는 방식입니다.  '''

        user_link_desc = f'''  
{user_link_desc}  
: 링크 상에 UID를 변경하여 개별 링크로 사용 가능합니다. (UID는 중복하여 사용 불가)  
ex) UID = **4909** (로그인 링크에서 4909를 입력해도 동일한 링크로 이동)  
https://pacific.surveys.nielseniq.com/survey/selfserve/548/XXX?list=9&UID=**4909**  '''
        
        user_link = f'''{user_link}  
{user_link_desc}  
&nbsp;'''


    pb_text = ''
    if pb is not None :
        pb_text = f'''**[PROJECT BOX]**  
[QC_PROJECT_BOX.xlsx]({pb})  
&nbsp;  
수정 사항은 Project box의 Survey Programming 시트에 업데이트 부탁드립니다.  
수정 사항 작성하실 때 Question/Detail/Stage/Type/Change code/Source/Opened Date는 필수로 입력 부탁드립니다.  
워딩 수정은 테스트 링크 하단의 edit 기능으로 수정 부탁드리며, 프로그래밍 언어는 수정되지 않도록 부탁드립니다.  
'''

    qc_text = ''
    if note_file is not None :
        # note_file 파일 존재 유무 검증
        if not os.path.exists(note_file) :
            raise FileNotFoundError(f'{note_file} is not found')
        
        with open(note_file, 'r', encoding='utf-8') as f :
            qc_text = f.read()
            qc_text = qc_text.split('\n')
            qc_text = '\n'.join(qc_text[1:])
        
        qc_text = f'''&nbsp;  
더불어, 하기 내용 확인 부탁드립니다.  
{qc_text}  
'''
        

    # 메일 종료 인사
    end_text = f'''감사합니다.'''
    if sp is not None :
        end_text = f'''감사합니다.  
{sp} 드림.'''


    # 메일 본문
    body_text = f'''
{head_text}
본 프로젝트 테스트 링크 전달드립니다.  
**[TEST LINK]**  
{test_link}  
{login_link}  
{user_link}    
{pb_text}  
{qc_text}  
{end_text}
    '''


    # 마크다운으로 출력
    display(Markdown(body_text))

    