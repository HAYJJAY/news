#!/usr/bin/env python3
import os
import sys
import time
import logging
import asyncio
import requests
from datetime import datetime
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
from playwright.async_api import async_playwright

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# 환경 변수에서 구글 시트 ID와 범위 가져오기
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')
RANGE_NAME = os.environ.get('RANGE_NAME')
CREDENTIALS_FILE = 'service-account-key.json'  # GitHub Secret에서 생성된 파일
N8N_WEBHOOK_URL = os.environ.get('N8N_WEBHOOK_URL', 'https://jayhay.app.n8n.cloud/webhook/article-content')

# 구글 시트 API 인증 및 서비스 생성
def get_sheets_service():
    """구글 시트 API 서비스 객체 생성"""
    try:
        creds = Credentials.from_service_account_file(
            CREDENTIALS_FILE,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        service = build('sheets', 'v4', credentials=creds)
        return service
    except Exception as e:
        logger.error(f"구글 시트 서비스 인증 실패: {e}")
        return None

# 구글 시트에서 데이터 가져오기
def get_sheet_data():
    """구글 시트에서 처리되지 않은 RSS 데이터 가져오기"""
    try:
        service = get_sheets_service()
        if not service:
            return None
            
        # 데이터 가져오기
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
        values = result.get('values', [])
        
        if not values:
            logger.info('구글 시트에서 데이터를 찾을 수 없습니다.')
            return []
            
        # 기존 헤더 정의 (n8n에서 업로드하는 열)
        headers = ['title', 'link', 'pubDate', 'content', 'contentSnippet', 'guid', 'isoDate', '_source']
        
        # 'publisher_url' 열을 위한 확장 헤더
        extended_headers = headers.copy()
        if len(values[0]) > len(headers):
            extended_headers.append('publisher_url')
        
        # 데이터프레임 생성
        df = pd.DataFrame(values, columns=extended_headers[:len(values[0])])
        
        # 'publisher_url' 열이 없거나 비어있는 항목만 필터링
        if 'publisher_url' in df.columns:
            unprocessed_df = df[df['publisher_url'].fillna('').str.strip() == '']
        else:
            # publisher_url 열이 없으면 모든 데이터를 처리 대상으로 간주
            unprocessed_df = df
        
        logger.info(f"총 {len(df)} 항목 중 발행자 URL이 없는 항목 {len(unprocessed_df)}개를 가져왔습니다.")
        
        return unprocessed_df
        
    except HttpError as e:
        logger.error(f"구글 시트 데이터 가져오기 실패: {e}")
        return None
    except Exception as e:
        logger.error(f"데이터 처리 중 오류 발생: {e}")
        return None

# 구글 시트 업데이트하기        
def update_sheet_with_publisher_url(guid, pub_url):
    """처리된 항목의 발행자 URL을 업데이트"""
    try:
        service = get_sheets_service()
        if not service:
            return False
            
        # 전체 데이터 가져오기
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
        values = result.get('values', [])
        
        if not values:
            logger.error('업데이트할 데이터를 찾을 수 없습니다.')
            return False
            
        # guid에 해당하는 행 찾기
        row_index = None
        guid_col_index = 5  # guid는 F열 (0-based index로는 5)
        
        for i, row in enumerate(values):
            if len(row) > guid_col_index and row[guid_col_index] == guid:
                row_index = i + 1  # 시트의 행 번호는 1부터 시작
                break
                
        if row_index is None:
            logger.error(f"GUID '{guid}'에 해당하는 행을 찾을 수 없습니다.")
            return False
        
        # 'publisher_url' 열 위치는 I열로 설정 (n8n이 8개 열까지만 업로드)
        range_name = f'Sheet1!I{row_index}'
        body = {'values': [[pub_url]]}
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name,
            valueInputOption='USER_ENTERED',
            body=body
        ).execute()
        
        logger.info(f"GUID '{guid}'의 발행자 URL을 업데이트했습니다: {pub_url}")
        return True
        
    except HttpError as e:
        logger.error(f"구글 시트 업데이트 실패: {e}")
        return False
    except Exception as e:
        logger.error(f"업데이트 중 오류 발생: {e}")
        return False

# URL 추출 함수
async def get_final_publisher_url(viewer_url):
    """Google News viewer URL에서 원본 발행자 URL 추출"""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=[
            '--no-sandbox',
            '--disable-blink-features=AutomationControlled',
            '--disable-features=IsolateOrigins,site-per-process'
        ])
        
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            viewport={'width': 1280, 'height': 800}
        )
        
        # Add cookies to bypass GDPR
        await context.add_cookies([
            {'name': 'CONSENT', 'value': 'YES+1', 'domain': '.google.com', 'path': '/'},
            {'name': 'CONSENT', 'value': 'YES+1', 'domain': '.news.google.com', 'path': '/'}
        ])
        
        page = await context.new_page()
        
        try:
            logger.info(f"Navigating to: {viewer_url}")
            response = await page.goto(viewer_url, wait_until='domcontentloaded', timeout=60000)
            
            if not response:
                logger.error(f"Failed to load page: {viewer_url}")
                return None
                
            if response.status >= 400:
                logger.error(f"HTTP error {response.status} for {viewer_url}")
                return None
            
            await asyncio.sleep(2)
            
            selectors = [
                'link[rel="alternate"]',
                'a[jsname="hXwDdf"]',
                'a[jscontroller]',
                'c-wiz a[rel="nofollow"]',
                'a[target="_blank"][rel="noopener"]',
                'div[jsname="gKDw6b"] a'
            ]
            
            pub_url = None
            for selector in selectors:
                try:
                    if await page.locator(selector).count() > 0:
                        pub_url = await page.get_attribute(selector, 'href', timeout=5000)
                        if pub_url:
                            logger.info(f"Found URL with selector '{selector}': {pub_url}")
                            break
                except Exception as e:
                    logger.debug(f"Selector '{selector}' failed: {str(e)}")
            
            if not pub_url:
                current_url = page.url
                if "news.google.com" not in current_url:
                    pub_url = current_url
                    logger.info(f"Found URL from redirect: {pub_url}")
            
            if not pub_url:
                logger.warning(f"Couldn't find publisher URL for: {viewer_url}")
                
            return pub_url
            
        except Exception as e:
            logger.error(f"Error extracting URL: {str(e)}")
            return None
        finally:
            await browser.close()

# N8N Webhook으로 데이터 전송하는 함수
def send_to_n8n_webhook(processed_data):
    """처리된 데이터를 N8N Webhook으로 전송"""
    try:
        if not processed_data:
            logger.info("전송할 데이터가 없습니다.")
            return False
            
        payload = {
            "processed_articles": processed_data
        }
        
        response = requests.post(N8N_WEBHOOK_URL, json=payload)
        
        if response.status_code == 200:
            logger.info(f"N8N Webhook으로 데이터 전송 성공: {len(processed_data)}건")
            return True
        else:
            logger.error(f"N8N Webhook 전송 실패: HTTP {response.status_code}, {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Webhook 전송 중 오류 발생: {str(e)}")
        return False

# 메인 처리 함수
async def process_articles(df):
    """시트에서 가져온 기사 데이터 처리"""
    results = []
    
    for _, row in df.iterrows():
        try:
            title = row['title']
            link = row['link']
            guid = row['guid']
            
            logger.info(f"Processing: {title}")
            
            # 원본 URL 추출
            pub_url = await get_final_publisher_url(link)
            
            if not pub_url:
                logger.warning(f"❌ {title}: Failed to find publisher URL")
                continue
            
            # 발행자 URL 업데이트
            update_sheet_with_publisher_url(guid, pub_url)
            
            # N8N에 전송할 데이터에 추가
            article_data = {
                "title": title,
                "original_link": link,
                "publisher_url": pub_url,
                "guid": guid
            }
            
            # 가능한 모든 필드 추가
            for col in df.columns:
                if col not in article_data and col in row:
                    article_data[col] = row[col]
                    
            results.append(article_data)
            
            logger.info(f"✅ Successfully processed: {title} -> {pub_url}")
            
        except Exception as e:
            logger.error(f"❌ Error processing article: {str(e)}")
    
    return results

# 메인 실행 코드
async def main():
    """메인 실행 함수"""
    logger.info("Google News URL 추출기 시작 (GitHub Actions)")
    
    # 데이터 가져오기
    df = get_sheet_data()
    
    if df is None or df.empty:
        logger.info("처리할 데이터가 없습니다.")
        return
    
    # 데이터 처리
    processed_results = await process_articles(df)
    
    # 결과 요약
    logger.info(f"요약: {len(processed_results)}개 항목 처리 성공")
    
    # N8N Webhook으로 데이터 전송
    if processed_results:
        send_to_n8n_webhook(processed_results)

if __name__ == "__main__":
    asyncio.run(main())
