import asyncio
import aiohttp
import aiofiles
from bs4 import BeautifulSoup
import csv, re

NUM_BOARDS = 99
ARTICLES_PER_BOARD = 99
CONCURRENT_REQUESTS = 20

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
COOKIE = {'over18': '1'}

async def fetch(session, url):
    async with session.get(url, headers=HEADERS) as r:
        return await r.text(encoding='utf-8', errors='ignore')

async def get_hotboards(session):
    html = await fetch(session, 'https://www.ptt.cc/bbs/hotboards.html')
    soup = BeautifulSoup(html, 'lxml')
    return [{'name': a.select_one('div.board-name').text.strip(),
             'url': 'https://www.ptt.cc' + a['href']}
            for a in soup.select('a.board')[:NUM_BOARDS]]

async def get_posts(session, board):
    url = board['url']
    if not url.endswith('.html'):
        url = url.rstrip('/') + '/index.html'
    html = await fetch(session, url)
    soup = BeautifulSoup(html, 'lxml')
    ent = soup.select('.r-ent')
    posts = []
    for entry in ent[:ARTICLES_PER_BOARD]:
        a = entry.select_one('.title a')
        if a:
            posts.append({
                'title': a.text.strip(),
                'link': 'https://www.ptt.cc' + a['href'],
                'author': entry.select_one('.author').text.strip()
            })
    return board, posts

async def get_detail(session, sem, post):
    async with sem:
        html = await fetch(session, post['link'])
        soup = BeautifulSoup(html, 'lxml')
        main = soup.select_one('#main-content')
        if not main:
            raise ValueError("No main-content")
        text = re.sub(r'\s+', ' ', main.text.strip())
        pushes = len(soup.select('.push'))
        return {**post, 'content': text, 'pushes': pushes}

async def save_csv(filename, data):
    keys = data[0].keys()
    async with aiofiles.open(filename, 'w', encoding='utf-8-sig', newline='') as f:
        await f.write(','.join(keys) + '\n')
        for row in data:
            line = ','.join('"' + str(row[k]).replace('"', '""') + '"' for k in keys)
            await f.write(line + '\n')

async def main():
    async with aiohttp.ClientSession(cookies=COOKIE) as session:
        boards = await get_hotboards(session)
        for board in boards:
            board, posts = await get_posts(session, board)
            print(f"🗂 {board['name']}: 找到 {len(posts)} 篇文章")
            if not posts:
                continue
            sem = asyncio.Semaphore(CONCURRENT_REQUESTS)
            tasks = [get_detail(session, sem, p) for p in posts]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            data = [r for r in results if not isinstance(r, Exception)]
            print(f"  ✅ 成功取得 {len(data)} 篇內文")
            if not data:
                continue
            await save_csv(f"{board['name']}.csv", data)
            print(f"  💾 已輸出 {board['name']}.csv\n")

if __name__ == '__main__':
    asyncio.run(main())
