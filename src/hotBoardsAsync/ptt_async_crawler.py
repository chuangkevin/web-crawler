"""
改進的非同步 PTT Playwright 爬蟲
增加重試機制和更好的錯誤處理
"""

import asyncio
import aiofiles
import csv
import re
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
import json

# 設定常數
NUM_BOARDS = 10
ARTICLES_PER_BOARD = 15
CONCURRENT_BOARDS = 3
CONCURRENT_ARTICLES = 5
PAGE_TIMEOUT = 30000
REQUEST_DELAY = 1
MAX_RETRIES = 2  # 最大重試次數
RETRY_DELAY = 3  # 重試延遲（秒）

class PTTPlaywrightCrawler:
    """PTT Playwright 非同步爬蟲類別"""
    
    def __init__(self):
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.stats = {
            'boards_processed': 0,
            'articles_crawled': 0,
            'articles_failed': 0,
            'articles_retried': 0,
            'errors': 0,
            'start_time': None,
            'end_time': None
        }

    async def __aenter__(self):
        """非同步上下文管理器進入"""
        await self.setup_browser()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """非同步上下文管理器退出"""
        await self.cleanup()

    async def setup_browser(self):
        """設定 Playwright 瀏覽器"""
        print("🔧 正在設定瀏覽器...")
        
        self.playwright = await async_playwright().start()
        
        # 啟動瀏覽器
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor',
                '--disable-gpu',
                '--no-first-run'
            ]
        )
        
        # 建立瀏覽器上下文
        self.context = await self.browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080},
            ignore_https_errors=True
        )
        
        # 設定 over18 cookie
        await self.context.add_cookies([{
            'name': 'over18',
            'value': '1',
            'domain': '.ptt.cc',
            'path': '/'
        }])
        
        print("✅ 瀏覽器設定完成")

    async def cleanup(self):
        """清理資源"""
        print("🧹 正在清理資源...")
        
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        
        print("✅ 資源清理完成")

    async def handle_page_setup(self, page: Page, retry_count: int = 0) -> bool:
        """處理頁面載入和年齡確認，增加重試機制"""
        try:
            # 根據重試次數調整超時時間
            timeout = 10000 + (retry_count * 5000)  # 重試時增加超時時間
            
            # 等待頁面載入
            await page.wait_for_load_state('networkidle', timeout=timeout)
            
            # 處理年齡確認
            age_confirm = page.locator('button:has-text("我同意，我已年滿十八歲")')
            if await age_confirm.count() > 0:
                await age_confirm.click()
                await page.wait_for_load_state('networkidle', timeout=5000)
            
            return True
        except Exception as e:
            if retry_count < MAX_RETRIES:
                print(f"⚠️ 頁面設定錯誤 (第{retry_count+1}次嘗試): {e}")
                return False
            else:
                print(f"❌ 頁面設定最終失敗: {e}")
                return False

    async def get_hotboards(self) -> List[Dict[str, str]]:
        """非同步獲取熱門看板"""
        print("🌐 正在獲取熱門看板...")
        
        page = await self.context.new_page()
        
        try:
            await page.goto('https://www.ptt.cc/bbs/hotboards.html', 
                           wait_until='domcontentloaded', timeout=PAGE_TIMEOUT)
            
            if not await self.handle_page_setup(page):
                return self.get_default_boards()
            
            # 等待看板列表載入
            await page.wait_for_selector('a.board', timeout=10000)
            
            # 獲取看板列表
            board_elements = await page.locator('a.board').all()
            print(f"🔍 找到 {len(board_elements)} 個看板")
            
            boards = []
            for element in board_elements[:NUM_BOARDS]:
                try:
                    board_name_elem = element.locator('div.board-name')
                    if await board_name_elem.count() > 0:
                        name = await board_name_elem.text_content()
                        href = await element.get_attribute('href')
                        
                        if name and href:
                            boards.append({
                                'name': name.strip(),
                                'url': 'https://www.ptt.cc' + href
                            })
                            
                except Exception as e:
                    print(f"⚠️ 解析看板元素錯誤: {e}")
                    continue
            
            print(f"✅ 成功獲取 {len(boards)} 個熱門看板")
            return boards if boards else self.get_default_boards()
            
        except Exception as e:
            print(f"❌ 獲取熱門看板失敗: {e}")
            return self.get_default_boards()
        finally:
            await page.close()

    def get_default_boards(self) -> List[Dict[str, str]]:
        """預設看板列表"""
        default_boards = [
            {'name': 'Gossiping', 'url': 'https://www.ptt.cc/bbs/Gossiping/'},
            {'name': 'Stock', 'url': 'https://www.ptt.cc/bbs/Stock/'},
            {'name': 'NBA', 'url': 'https://www.ptt.cc/bbs/NBA/'},
            {'name': 'Baseball', 'url': 'https://www.ptt.cc/bbs/Baseball/'},
            {'name': 'C_Chat', 'url': 'https://www.ptt.cc/bbs/C_Chat/'},
            {'name': 'PC_Shopping', 'url': 'https://www.ptt.cc/bbs/PC_Shopping/'},
            {'name': 'DC_SALE', 'url': 'https://www.ptt.cc/bbs/DC_SALE/'},
            {'name': 'MobileComm', 'url': 'https://www.ptt.cc/bbs/MobileComm/'},
            {'name': 'Lifeismoney', 'url': 'https://www.ptt.cc/bbs/Lifeismoney/'},
            {'name': 'car', 'url': 'https://www.ptt.cc/bbs/car/'}
        ]
        print(f"📋 使用預設看板列表 ({len(default_boards)} 個)")
        return default_boards[:NUM_BOARDS]

    async def get_board_posts(self, board: Dict[str, str]) -> Tuple[Dict[str, str], List[Dict[str, str]]]:
        """非同步獲取看板文章列表"""
        page = await self.context.new_page()
        
        try:
            url = board['url']
            if not url.endswith('.html'):
                url = url.rstrip('/') + '/index.html'
            
            await page.goto(url, wait_until='domcontentloaded', timeout=PAGE_TIMEOUT)
            
            if not await self.handle_page_setup(page):
                return board, []
            
            # 等待文章列表載入
            await page.wait_for_selector('.r-ent', timeout=10000)
            
            # 獲取文章列表
            article_elements = await page.locator('.r-ent').all()
            
            posts = []
            for element in article_elements[:ARTICLES_PER_BOARD]:
                try:
                    # 獲取標題連結
                    title_link = element.locator('.title a')
                    if await title_link.count() == 0:
                        continue
                    
                    title = await title_link.text_content()
                    href = await title_link.get_attribute('href')
                    
                    if not title or not href:
                        continue
                    
                    # 獲取作者
                    author_elem = element.locator('.author')
                    author = await author_elem.text_content() if await author_elem.count() > 0 else ""
                    
                    # 獲取日期
                    date_elem = element.locator('.date')
                    date = await date_elem.text_content() if await date_elem.count() > 0 else ""
                    
                    posts.append({
                        'title': title.strip(),
                        'link': 'https://www.ptt.cc' + href,
                        'author': author.strip(),
                        'date': date.strip(),
                        'board': board['name']
                    })
                    
                except Exception as e:
                    print(f"⚠️ 解析文章錯誤: {e}")
                    continue
            
            return board, posts
            
        except Exception as e:
            print(f"❌ 獲取 {board['name']} 文章失敗: {e}")
            return board, []
        finally:
            await page.close()

    async def get_article_detail_with_retry(self, sem: asyncio.Semaphore, post: Dict[str, str]) -> Dict[str, str]:
        """獲取文章詳細內容（帶重試機制）"""
        async with sem:
            for retry_count in range(MAX_RETRIES + 1):
                try:
                    page = await self.context.new_page()
                    
                    try:
                        # 加載文章頁面
                        await page.goto(post['link'], wait_until='domcontentloaded', timeout=PAGE_TIMEOUT)
                        
                        # 處理頁面設定（帶重試）
                        if not await self.handle_page_setup(page, retry_count):
                            if retry_count < MAX_RETRIES:
                                print(f"🔄 重試 {post['title'][:30]}... (第{retry_count+1}次)")
                                self.stats['articles_retried'] += 1
                                await page.close()
                                await asyncio.sleep(RETRY_DELAY)
                                continue
                            else:
                                return {**post, 'content': '', 'pushes': 0, 'status': 'failed', 'retry_count': retry_count}
                        
                        # 等待主要內容載入
                        await page.wait_for_selector('#main-content', timeout=15000)
                        
                        # 獲取文章內容
                        main_content = page.locator('#main-content')
                        if await main_content.count() > 0:
                            content_text = await main_content.text_content()
                            content = re.sub(r'\s+', ' ', content_text.strip()) if content_text else ""
                        else:
                            content = ""
                        
                        # 計算推文數
                        push_elements = await page.locator('.push').all()
                        pushes = len(push_elements)
                        
                        # 添加時間戳
                        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        
                        result = {
                            **post,
                            'content': content[:1000],  # 限制內容長度
                            'pushes': pushes,
                            'status': 'success',
                            'crawl_time': timestamp,
                            'retry_count': retry_count
                        }
                        
                        if retry_count > 0:
                            print(f"✅ 重試成功: {post['title'][:30]} (第{retry_count+1}次嘗試)")
                        
                        # 請求間延遲
                        await asyncio.sleep(REQUEST_DELAY)
                        
                        return result
                        
                    finally:
                        await page.close()
                        
                except Exception as e:
                    if retry_count < MAX_RETRIES:
                        print(f"⚠️ 獲取內容錯誤 {post['title'][:30]} (第{retry_count+1}次): {e}")
                        self.stats['articles_retried'] += 1
                        await asyncio.sleep(RETRY_DELAY)
                        continue
                    else:
                        print(f"❌ 最終失敗: {post['title'][:30]}: {e}")
                        self.stats['errors'] += 1
                        return {
                            **post, 
                            'content': '', 
                            'pushes': 0, 
                            'status': 'error',
                            'error': str(e),
                            'retry_count': retry_count
                        }
            
            # 不應該到達這裡
            return {**post, 'content': '', 'pushes': 0, 'status': 'failed', 'retry_count': MAX_RETRIES}

    async def save_data_async(self, filename: str, data: List[Dict]) -> bool:
        """非同步儲存資料到 CSV"""
        if not data:
            print(f"⚠️ 沒有資料可儲存到 {filename}")
            return False
        
        try:
            # 使用 aiofiles 進行非同步檔案寫入
            async with aiofiles.open(filename, 'w', encoding='utf-8-sig', newline='') as f:
                # 寫入標題行
                keys = data[0].keys()
                header = ','.join(f'"{key}"' for key in keys) + '\n'
                await f.write(header)
                
                # 寫入資料行
                for row in data:
                    values = []
                    for key in keys:
                        value = str(row.get(key, '')).replace('"', '""')
                        values.append(f'"{value}"')
                    line = ','.join(values) + '\n'
                    await f.write(line)
                    
            print(f"💾 已非同步儲存 {len(data)} 筆資料到 {filename}")
            return True
            
        except Exception as e:
            print(f"❌ 儲存 CSV 錯誤: {e}")
            return False

    async def save_summary_async(self, boards_data: Dict) -> bool:
        """非同步儲存爬取摘要"""
        try:
            # 將 datetime 對象轉換為字符串
            stats_copy = self.stats.copy()
            if stats_copy.get('start_time'):
                stats_copy['start_time'] = stats_copy['start_time'].isoformat()
            if stats_copy.get('end_time'):
                stats_copy['end_time'] = stats_copy['end_time'].isoformat()
            
            summary = {
                'crawl_time': datetime.now().isoformat(),
                'stats': stats_copy,
                'boards': {}
            }
            
            for board_name, articles in boards_data.items():
                successful = len([a for a in articles if a.get('status') == 'success'])
                failed = len([a for a in articles if a.get('status') in ['failed', 'error']])
                retried = len([a for a in articles if a.get('retry_count', 0) > 0])
                
                summary['boards'][board_name] = {
                    'total_articles': len(articles),
                    'successful_articles': successful,
                    'failed_articles': failed,
                    'retried_articles': retried
                }
            
            async with aiofiles.open('crawl_summary_improved.json', 'w', encoding='utf-8') as f:
                await f.write(json.dumps(summary, ensure_ascii=False, indent=2))
                
            print("📊 已儲存改進的爬取摘要到 crawl_summary_improved.json")
            return True
            
        except Exception as e:
            print(f"❌ 儲存摘要錯誤: {e}")
            return False

    async def process_board(self, sem: asyncio.Semaphore, board: Dict[str, str]) -> Tuple[str, List[Dict]]:
        """非同步處理單一看板"""
        async with sem:
            print(f"🎯 開始處理看板: {board['name']}")
            
            try:
                # 獲取文章列表
                board_result, posts = await self.get_board_posts(board)
                print(f"📋 {board['name']}: 找到 {len(posts)} 篇文章")
                
                if not posts:
                    return board['name'], []
                
                # 獲取文章詳細內容
                article_sem = asyncio.Semaphore(CONCURRENT_ARTICLES)
                tasks = [self.get_article_detail_with_retry(article_sem, post) for post in posts]
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # 過濾成功的結果
                articles = []
                for result in results:
                    if isinstance(result, Exception):
                        print(f"⚠️ 文章處理異常: {result}")
                        self.stats['errors'] += 1
                    else:
                        articles.append(result)
                        if result.get('status') == 'success':
                            self.stats['articles_crawled'] += 1
                        else:
                            self.stats['articles_failed'] += 1
                
                self.stats['boards_processed'] += 1
                successful_count = len([a for a in articles if a.get('status') == 'success'])
                print(f"✅ {board['name']}: 成功處理 {successful_count}/{len(articles)} 篇文章")
                
                return board['name'], articles
                
            except Exception as e:
                print(f"❌ 處理看板 {board['name']} 時發生錯誤: {e}")
                self.stats['errors'] += 1
                return board['name'], []

    async def crawl_all_boards(self) -> Dict[str, List[Dict]]:
        """非同步爬取所有看板"""
        self.stats['start_time'] = datetime.now()
        print("🚀 開始非同步爬取所有看板...")
        
        # 獲取熱門看板
        boards = await self.get_hotboards()
        
        if not boards:
            print("⚠️ 沒有找到任何看板")
            return {}
        
        print(f"📊 將處理 {len(boards)} 個看板，每個看板 {ARTICLES_PER_BOARD} 篇文章")
        print(f"🔄 設定重試機制：最多重試 {MAX_RETRIES} 次")
        
        # 建立信號量控制並發數
        board_sem = asyncio.Semaphore(CONCURRENT_BOARDS)
        
        # 並發處理所有看板
        tasks = [self.process_board(board_sem, board) for board in boards]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 整理結果
        boards_data = {}
        for result in results:
            if isinstance(result, Exception):
                print(f"⚠️ 看板處理異常: {result}")
                continue
            
            board_name, articles = result
            if articles:
                boards_data[board_name] = articles
        
        self.stats['end_time'] = datetime.now()
        return boards_data

    async def run(self) -> bool:
        """執行完整的爬取流程"""
        try:
            print("🌟 啟動改進的非同步 PTT 爬蟲...")
            
            # 爬取所有看板
            boards_data = await self.crawl_all_boards()
            
            if not boards_data:
                print("⚠️ 沒有成功爬取任何資料")
                return False
            
            # 並發儲存所有資料
            save_tasks = []
            for board_name, articles in boards_data.items():
                filename = f"{board_name}_improved.csv"
                save_tasks.append(self.save_data_async(filename, articles))
            
            # 添加摘要儲存任務
            save_tasks.append(self.save_summary_async(boards_data))
            
            # 等待所有儲存任務完成
            save_results = await asyncio.gather(*save_tasks, return_exceptions=True)
            
            # 顯示統計資訊
            elapsed = self.stats['end_time'] - self.stats['start_time']
            print(f"\n🎉 爬取完成！")
            print(f"📊 統計資訊:")
            print(f"   ⏱️  執行時間: {elapsed}")
            print(f"   📋 處理看板: {self.stats['boards_processed']}")
            print(f"   ✅ 成功文章: {self.stats['articles_crawled']}")
            print(f"   ❌ 失敗文章: {self.stats['articles_failed']}")
            print(f"   🔄 重試文章: {self.stats['articles_retried']}")
            print(f"   💥 異常錯誤: {self.stats['errors']}")
            
            return True
            
        except Exception as e:
            print(f"❌ 爬取流程錯誤: {e}")
            return False


async def main():
    """主要執行函數"""
    try:
        async with PTTPlaywrightCrawler() as crawler:
            success = await crawler.run()
            return 0 if success else 1
            
    except KeyboardInterrupt:
        print("\n👋 程式被使用者中斷")
        return 1
    except Exception as e:
        print(f"\n❌ 程式執行錯誤: {e}")
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(asyncio.run(main()))
