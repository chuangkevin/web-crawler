from requests_html import HTMLSession
from pyquery import PyQuery as pq
import csv
import time

# 建立 HTML Session 並通過 PTT 八卦版的年齡驗證
session = HTMLSession()
session.cookies.set('over18', '1')

def fetch_index(url):
    response = session.get(url)
    response.encoding = 'utf-8'  # 避免亂碼
    return response.text

def parse_index(html):
    doc = pq(html)
    for entry in doc('.r-ent').items():
        title_elem = entry('.title a')
        if not title_elem:
            continue
        title = title_elem.text()
        link = title_elem.attr('href')
        author = entry('.author').text()
        yield {'title': title, 'link': link, 'author': author}

def fetch_post(link):
    response = session.get('https://www.ptt.cc' + link)
    response.encoding = 'utf-8'
    return response.text

def parse_post(html):
    doc = pq(html)
    content = doc('#main-content').text()
    pushes = len(list(doc('.push').items()))  # 把 generator 轉 list 才能用 len
    return {'content': content, 'pushes': pushes}

def run():
    data = []
    index_html = fetch_index('https://www.ptt.cc/bbs/Gossiping/index.html')
    for meta in parse_index(index_html):
        print(f"Processing: {meta['title']}")
        try:
            post_html = fetch_post(meta['link'])
            details = parse_post(post_html)
            data.append({**meta, **details})
            time.sleep(0.5)  # 小心被 PTT 擋掉，稍微睡一下
        except Exception as e:
            print(f"Error processing post: {e}")

    # 儲存結果成 CSV
    with open('ptt_output.csv', 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

if __name__ == '__main__':
    run()
