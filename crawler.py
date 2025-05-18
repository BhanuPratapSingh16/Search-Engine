import requests
from bs4 import BeautifulSoup
from collections import deque
import urllib.robotparser
from urllib.parse import urlparse, urljoin
import time
import sqlite3
import json
from tldextract import extract


# HELPER FUNCTIONS
# Function to check if a URL is valid
def is_valid_url(url):
    return url.startswith("http")

# Function to check if a URL allows crawling using robots.txt
def can_crawl(url, user_agent="MyCrawler"):
    parsed = urlparse(url)
    
    base_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url(base_url)
    try:
        rp.read()
        return rp.can_fetch(user_agent, url), rp.crawl_delay(user_agent)
    except:
        return False, 0
    

# Function to extract domain
def extract_domain(url):
    ext = extract(url)
    domain = f"{ext.domain}.{ext.suffix}"
    return domain

# Function to push data into sqlite
def save_data(page_data, cursor):
    url  = page_data["url"]
    domain = extract_domain(url)
    title = page_data["title"]
    keywords = json.dumps(page_data["keywords"])
    paragraphs = json.dumps(page_data["paragraphs"])
    headings = json.dumps(page_data["headings"])
    full_text = page_data["full_text"]

    cursor.execute('''INSERT OR IGNORE INTO url_data(url, domain, title, keywords, paragraphs, headings, full_text) VALUES(?,?,?,?,?,?,?)''', (url, domain, title, keywords, paragraphs, headings, full_text))
    conn.commit()
    

# Global variables
urls_to_visit = deque()
visited_urls = set()
crawl_limit = 250
crawler_count = 0
max_pages_per_domain = 15
domain_crawl_count = {}

urls_to_visit.append("https://example.com")
urls_to_visit.append("https://books.toscrape.com") 
urls_to_visit.append("https://quotes.toscrape.com")
urls_to_visit.append("https://httpbin.org")
urls_to_visit.append("https://www.wikipedia.org")
urls_to_visit.append("https://openlibrary.org")
urls_to_visit.append("https://scrapethissite.com")
urls_to_visit.append("https://www.scrapingcourse.com/ecommerce/")
urls_to_visit.append("https://www.scrapingcourse.com/pagination")
urls_to_visit.append("https://www.nytimes.com/international/")
urls_to_visit.append("https://opentdb.com/browse.php")
urls_to_visit.append("https://hianimez.to/")
urls_to_visit.append("https://www.amazon.in/?&tag=googhydrabk1-21&ref=pd_sl_5szpgfto9i_e&adgrpid=155259813593&hvpone=&hvptwo=&hvadid=674893540034&hvpos=&hvnetw=g&hvrand=15735365193666244267&hvqmt=e&hvdev=c&hvdvcmdl=&hvlocint=&hvlocphy=9302138&hvtargid=kwd-64107830&hydadcr=14452_2316413&gad_source=1")
urls_to_visit.append("https://www.espn.in/")
urls_to_visit.append("https://indianexpress.com/section/sports/")
urls_to_visit.append("https://www.skysports.com/")
urls_to_visit.append("https://sports.ndtv.com/")
urls_to_visit.append("https://indianexpress.com/")
urls_to_visit.append("https://www.aljazeera.com/news/")

for url in urls_to_visit:
    domain_crawl_count[extract_domain(url)] = 0

# Connecting to database
conn = sqlite3.connect("crawl_data.db")
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS url_data
          (id INTEGER PRIMARY KEY AUTOINCREMENT,
          url TEXT UNIQUE,
          domain TEXT,
          title TEXT,
          keywords TEXT,
          paragraphs TEXT,
          headings TEXT,
          full_text TEXT)''')


while urls_to_visit and crawler_count < crawl_limit:
    url = urls_to_visit.popleft()

    if url in visited_urls or not is_valid_url(url):
        continue

    allowed, delay = can_crawl(url)
    if allowed:
        visited_urls.add(url)
        crawler_count += 1
        print(f"Visiting: {url} ({crawler_count}/{crawl_limit})")

        if delay is not None:
            time.sleep(delay)
        else:
            time.sleep(1.5)
        
        # Fetching and parsing the page
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")

            # Extracting data
            page_data = {}

            # Title and URL
            page_data["title"] = soup.title.string if soup.title else "No title"
            page_data["url"] = url

            # Paragraphs
            divs = soup.find_all(["p", "article", "main"])
            page_data["paragraphs"] = [div.get_text().lower() for div in divs]

            # Headings
            headings = soup.find_all(["h1", "h2", "h3", "header"])
            page_data["headings"] = [heading.get_text(strip=True).lower() for heading in headings]

            # Keywords
            keyword_tags = soup.find_all("meta", attrs={"name":"keywords"})
            keywords = [tag["content"] for tag in keyword_tags if "content" in tag.attrs]
            page_data["keywords"] = ", ".join(keywords) if keywords else None

            # Full plain text
            page_data["full_text"] = soup.get_text(separator='\n', strip=True)

            # Saving the data in the database
            save_data(page_data, c)

            # Extracting links
            links = soup.find_all("a", href=True)

            for link in links:
                next_url = link["href"]
                next_url = urljoin(url, link["href"])
                if not is_valid_url(next_url) or next_url in visited_urls:
                    continue

                domain = extract_domain(next_url)
                if domain in domain_crawl_count.keys():
                    if domain_crawl_count[domain] < max_pages_per_domain:
                        urls_to_visit.append(next_url)
                        domain_crawl_count[domain] += 1

    else:
        print(f"Not allowed to crawl: {url}")


conn.commit()
conn.close()