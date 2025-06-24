from requests_html import HTMLSession
from pyquery import PyQuery as pq
import csv
import time
import re

# ====== 你可以調整的參數 ======
NUM_BOARDS = 99        # 抓幾個熱門看板
ARTICLES_PER_BOARD = 20  # 每個看板抓幾篇
# ============================

session = HTMLSession()
session.cookies.set('over18', '1')


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}

def fetch(url):
    r = session.get(url, headers=HEADERS)
    r.encoding = 'utf-8'
    return r.text

def get_hotboards():
    html = fetch('https://www.ptt.cc/bbs/hotboards.html')
    doc = pq(html)
    boards = []
    for a in doc('a.board').items():
        name = a.find('div.board-name').text()
        href = a.attr('href')
        if name and href:
            boards.append({'name': name, 'url': 'https://www.ptt.cc' + href})
        if len(boards) >= NUM_BOARDS:
            break
    return boards

def get_page_url(doc):
    arrow = doc('div.btn-group-paging a:contains("上頁")')
    return 'https://www.ptt.cc' + arrow.attr('href') if arrow else None

def get_posts(board_url, max_articles=20, max_pages=1):
    posts = []
    url = board_url if board_url.endswith('/index.html') else board_url + '/index.html'
    for _ in range(max_pages):
        html = fetch(url)
        doc = pq(html)
        for entry in doc('.r-ent').items():
            a = entry.find('.title a')
            if not a:
                continue
            posts.append({
                'title': a.text(),
                'link': a.attr('href'),
                'author': entry.find('.author').text()
            })
            if len(posts) >= max_articles:
                return posts
        next_url = get_page_url(doc)
        if not next_url:
            break
        url = next_url
        time.sleep(0.5)
    return posts

def clean_text(text):
    return re.sub(r'\s+', ' ', text.strip())

def get_post_details(link):
    url = 'https://www.ptt.cc' + link
    html = fetch(url)
    doc = pq(html)
    content = doc('#main-content').text()
    if not content:
        raise ValueError("Empty content in post")
    pushes = len(list(doc('.push').items()))
    return clean_text(content), pushes

def main():
    boards = get_hotboards()
    for b in boards:
        print(f"📌 board → {b['name']}")
        posts = get_posts(b['url'], max_articles=ARTICLES_PER_BOARD, max_pages=1)
        data = []
        print(f"  🔍 Found {len(posts)} posts")
        for p in posts:
            try:
                content, pushes = get_post_details(p['link'])
                data.append({
                    'title': p['title'],
                    'author': p['author'],
                    'link': 'https://www.ptt.cc' + p['link'],
                    'pushes': pushes,
                    'content': content
                })
                time.sleep(0.3)
            except Exception as e:
                print(f"  ⚠️  Error in post: {p['title']} → {e}")
        if not data:
            print(f"  ⚠️  No data for {b['name']}, skipping\n")
            continue
        fname = f"{b['name']}.csv"
        with open(fname, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        print(f"  ✅ Saved to {fname}\n")

if __name__ == '__main__':
    main()
