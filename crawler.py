from urllib import parse
import requests, time, queue, json
from bs4 import BeautifulSoup as bs
from urllib.parse import urljoin, urlparse, urlunparse
from collections import defaultdict
from urllib.robotparser import RobotFileParser
import urllib.request
import threading

class RFPTimeout(RobotFileParser):

    def __init__(self, url='', timeout=5):
        super().__init__(url)
        self.timeout = timeout

    def read(self):
        try:
            f = urllib.request.urlopen(self.url, timeout=self.timeout)
        except urllib.error.HTTPError as err:
            if err.code in (401, 403):
                self.disallow_all = True
            elif err.code >= 400:
                self.allow_all = True
        else:
            raw = f.read()
            self.parse(raw.decode("utf-8").splitlines())

def canonicalize(url, base_url):
    if len(url) <= 1: return None
    if url[0] == '/' and url[1] != '/':
        url = urljoin(base_url, url)
        #print('relative', url)
    parsed_url = urlparse(url)
    scheme = parsed_url.scheme.lower()
    domain = parsed_url.netloc.lower()
    if ':' in domain: domain = domain[:domain.index(':')]
    
    # fix the path
    path = parsed_url.path
    if '//' in path: path =  "/".join(path.split('//'))
    if ':' in path: path = path[:path.index(':')]

    def filter_words(path):
        avoid_words = {'gettyimages', 'video', 'coupons', 'password', 'privacy', 'terms', 'about us', 'store',\
            'sports', 'cookie', 'cookies', 'accessibility', 'coronavirus', 'whatsapp', 'mailto',\
            'covid', ".jpg", ".jpeg", ".gif", ".png", ".mp4", ".pdf", "none", ".php", ".ppt", ".doc"}
        for phrase in avoid_words:
            if phrase in path:
                return False
        return True

    if filter_words(path) == False: return None
    if scheme == '': scheme = 'https'
    elif scheme != 'http' and scheme != 'https':
        #print('alternate scheme', scheme)
        return None
    return urlunparse((scheme, domain, path, '', '', ''))

def get_outlinks(soup, base_url):
    outlinks = []
    all_links = set(soup.find_all("a", href=True))
    _outs = set()

    def filter_link(link, _outs):
        link = link.lower()
        if link in _outs or link == 'none' or link == None or link == '' or link[0] == '#':
            return False
        avoid_words = {'facebook', 'gettyimages', 'video', 'image', 'job', 'password', 'privacy', 'terms', 'about us', '.mp4',\
            'coronavirus', 'covid', ".jpg", ".jpeg", ".gif", ".png", ".pdf", ".php", ".ppt", ".doc"\
                'accesibility', 'coupons', 'blog', 'russia', 'ukraine', '.mp3'}
        for a in avoid_words:
            if a in link:
                return False
        if link.count('%') > 5: return False
        return True

    for link in all_links:
        s = link.string
        if s != None: s = s.strip()
        link = str(link.get('href'))
        if link == "": continue
        if link[-1] != '/': link += '/'
        
        if filter_link(link, _outs) == False: continue

        link = canonicalize(link, base_url)
        if link == None: continue
        outlinks.append((link, s))
        _outs.add(link)
    #print(outlinks)
    return set(outlinks)

def get_text(soup):
    text = []
    for sup in soup.find_all('sup'):
        sup.extract()

    paragraphs = soup.find_all('p')
    if len(paragraphs) == 0: return ''
    for paragraph in paragraphs:
        string = paragraph.get_text().replace(u'\xa0', u' ')
        text.append(string)

    return " ".join(text)

def get_prio(url, anchor_text, curr_prio):
    if anchor_text is None: anchor_text = ""
    domain, path = urlparse(url).netloc.lower(), urlparse(url).path.lower()
    dot_gov = 1 if ".gov" in url else 0
    if ('hurricane' in path or 'hurricane' in domain) and ('sandy' in path or 'sandy' in domain):
        return -95 - 4*dot_gov 
        #+ 1*curr_prio
    keywords = {'katrina': 5, 'hurricane': 2, 'storm' : 1, 'damage': 1, 'tropical': 1, 'rain': 0.5, 'atlantic': 1}
    matches = sum([val if kw in domain or kw in path else 0 \
               for kw, val in keywords.items()])/sum(keywords.values())
    anchor_matches = sum([val if kw in anchor_text else 0 \
               for kw, val in keywords.items()])/sum(keywords.values()) 
    if matches == 0: return -1
    return -1 * (50 * matches + 30 * anchor_matches + 19 * dot_gov) 
    #+ 1 * curr_prio
    
def dump_url_data(url_data, raw_htmls, file_no):
    # Write 500 raw htmls and webpages to a file
    raw_html_filename = f'./raw_html_k_{file_no}'
    with open(raw_html_filename, 'w') as f:
        f.write(json.dumps(raw_htmls))
    
    url_data_filename = f'./webpages_k_{file_no}'
    with open(url_data_filename, 'a') as url_f:
        for f in url_data:
            title_text = f["title"] if f["title"] is not None else ""
            webpage_str = f'<DOC>\n<DOCNO>{f["url"]}<\DOCNO>\n' + \
                f'<HEAD>{title_text}<\HEAD>\n' + \
                f'<TEXT>\n{f["text"]}<\TEXT>\n<\DOC>\n'
            url_f.write(webpage_str)

def dump_outlinks(url_data):
    with open('outlinks_k.csv', 'a') as outlinks_file:
        for url in url_data:
            outlinks_file.write(url['url'] + ", ")
            outlinks_file.write(", ".join(list(map(lambda x : x[0], url['outlinks']))))
            outlinks_file.write('\n')
    
def dump_inlinks(inlinks):
    with open('inlinks_k.csv', 'a') as inlinks_file:
        for url, inlinks_list in inlinks.items():
            inlinks_file.write(url + ", ")
            inlinks_file.write(", ".join(inlinks_list))
            inlinks_file.write('\n')
    
def parse_url(url):
    parsed = urlparse(url)
    if parsed.scheme == '':
        parsed.scheme = 'http'
    return parsed.scheme, parsed.netloc, parsed.path 

def request(url):
    default_ret = 0, 0
    try:
        r = requests.get(url, timeout=(2, 20))
        start_tags = r.text[:100]
    except Exception:
        print('start exception')
        return default_ret

    if r.status_code != 200: 
        #print('status code', r.status_code)
        return default_ret
    
    headers = r.headers
    if 'html' not in start_tags and 'htm' not in url: 
        print('skipping here because html line 160')
        ctype = headers.get('Content-Type', '')
        if ctype == '': return default_ret
    
    if 'lang=' in start_tags: 
        lang = start_tags.find("en")
        if lang < 0: 
            l = headers.get('content-language', '')
            if l == '': 
                print('lang problem')
                return default_ret
    
    soup = bs(r.text, "html.parser")
    text = get_text(soup)
    #if len(text) <= 250: return default_ret
    title = soup.title
    
    # for a in avoid_words: 
    #     if title is not None and a in title: return default_ret
    if title is None: title = ''
    outlinks = get_outlinks(soup, url)
    #print(len(outlinks), 'for url', url)
    return {'url': url, 'title': str(title), 'text': text, 'outlinks': outlinks}, {'raw_html': soup.text}

def wait_if_needed(rp, curr_scheme, curr_domain):
    global waiting_dict
    rpd = urljoin(f"{curr_scheme}://{curr_domain}", 'robots.txt')
    rp.set_url(rpd)
    rp.read()
    wait = 0
    if rp.crawl_delay('*') is None:
        waiting_dict.add(curr_domain)
    if rp.can_fetch('*', rpd) == False: 
        wait = rp.crawl_delay('*')
        if wait > 0: 
            print('WAITING', wait)
            time.sleep(wait)
    time.sleep(1)
    rp.modified()
        
def crawler(rp):
    def crawl():
        global url_q
        global file_no
        global url_data
        global raw_htmls
        #global inlinks
        global crawled
        global visited
        global waiting_dict
        while crawled < 40500:
            curr_prio, curr_url = url_q.get_nowait()
            curr_scheme, curr_domain, curr_path = parse_url(curr_url)

            if str(curr_domain) + str(curr_path) in visited: 
                continue

            visited.add(str(curr_domain) + str(curr_path))

            if curr_domain not in waiting_dict:
                try:
                    wait_if_needed(rp, curr_scheme, curr_domain)
                except Exception:
                    time.sleep(1)
                    continue
            #if res == False: waiting_dict.add(curr_domain)

            curr_url_data, curr_raw_html = request(curr_url)
        
            if curr_url_data == 0: continue

            url_data.append(curr_url_data)
            raw_htmls.append(curr_raw_html)

            for outlink in curr_url_data['outlinks']:
                # print(outlink)
                link = outlink[0]
                anchor = outlink[1]
                #inlinks[link].add(curr_url)
                url_q.put((get_prio(link, anchor, curr_prio), link))
            
            lock.acquire()
            crawled += 1
            #if crawled%500 == 0: print(f"CRAWLED SO MANY DOCS {crawled}")
            lock.release()

            dump_lock.acquire()
            if len(url_data) >= 500:
                print(f"writing url_data {file_no}")
                dump_url_data(url_data, raw_htmls, file_no)
                dump_outlinks(url_data)
                url_data = []
                raw_htmls = []
                file_no += 1
            dump_lock.release()

            print(f"done crawling {curr_url}")
            

        final_dump_lock.acquire()
        if len(url_data) > 0:
            dump_url_data(url_data, raw_htmls, file_no)
            dump_outlinks(url_data)
            url_data = []
            #dump_inlinks(inlinks)
        final_dump_lock.release()
    return crawl

if __name__ == "__main__":
    #seed1 = "https://en.wikipedia.org/wiki/List_of_Atlantic_hurricane_records"
    #seed2 = "http://www.livescience.com/22522-hurricane-katrina-facts.html"
    # seed3 = "http://www.cnn.com/2013/08/23/us/hurricane-katrina-statistics-fast-facts/"
    # seed4 = "http://en.wikipedia.org/wiki/Hurricane_Katrina"
    seed1 = "http://www.nhc.noaa.gov/outreach/history/"
    seed2 = "https://en.wikipedia.org/wiki/List_of_United_States_hurricanes"
    seed3 = "http://en.wikipedia.org/wiki/Hurricane_Sandy"
    seed4 = "https://www.fema.gov/sandy-recovery-office"
    seed5 = "http://en.wikipedia.org/wiki/Effects_of_Hurricane_Sandy_in_New_York"
    lock = threading.Lock()
    dump_lock = threading.Lock()
    final_dump_lock = threading.Lock()
    rp = RFPTimeout()
    url_q = queue.PriorityQueue()
    seeds = [seed1, seed3, seed2, seed5]
    for url in seeds: 
        url_q.put((-101, url))
    print('initialized q')

    file_no = 0
    url_data = []
    raw_htmls = []
    #inlinks = defaultdict(set)
    crawled = 0
    visited = set()
    waiting_dict = {'weather.gov'}

    for s in seeds: 
        curr_scheme, curr_domain, curr_path = parse_url(s)
        visited.add(str(curr_domain) + str(curr_path))
    
        try:
            wait_if_needed(rp, curr_scheme, curr_domain)
        except Exception:
            continue
       
        # try:
        curr_url_data, curr_raw_html = request(s)
        # except Exception:
        #     continue
        
        if curr_url_data == 0: 
            print('url 0', s)
            continue

        url_data.append(curr_url_data)
        raw_htmls.append(curr_raw_html)
        for outlink in curr_url_data['outlinks']:
            link = outlink[0]
            anchor = outlink[1]
            #inlinks[link].add(s)
            url_q.put((get_prio(link, anchor, -101), link))
        print('DONE SEED', s)

    #time.sleep(30)
    print('starting to thread')
    threads = []
    for i in range(4):
        t = threading.Thread(target=crawler(rp))
        t.start()
        threads.append(t)
    
    for t in threads:
        t.join()

        
