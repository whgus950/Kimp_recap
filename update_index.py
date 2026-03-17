"""
update_index.py — GitHub Pages 및 정적 호스팅용 인덱스 생성기

`data/` 폴더 안의 YYYY-MM-DD.json 파일 목록을 읽어
프론트엔드가 참고할 수 있도록 `data/index.json`으로 만듭니다.
이 스크립트는 GitHub Actions 등에서 매일 데이터를 수집한 직후 실행되어야 합니다.
"""

import os
import json

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

def update_index():
    if not os.path.exists(DATA_DIR):
        print(f"Data directory not found: {DATA_DIR}")
        return

    # YYYY-MM-DD.json 형태의 파일들만 찾습니다.
    json_files = []
    for f in os.listdir(DATA_DIR):
        if f.endswith('.json') and f != 'index.json':
            json_files.append(f.replace('.json', ''))

    # 최신 날짜가 위로 오게 내림차순 정렬
    json_files.sort(reverse=True)

    index_path = os.path.join(DATA_DIR, 'index.json')
    with open(index_path, 'w', encoding='utf-8') as f:
        json.dump(json_files, f, ensure_ascii=False, indent=2)

    print(f"Updated index.json with {len(json_files)} dates.")

if __name__ == '__main__':
    update_index()
