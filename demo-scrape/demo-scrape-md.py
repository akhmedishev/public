import os
import sys
import time
import datetime as dt
import logging
import json
import scrapy
import random
from scrapy.crawler import CrawlerProcess
from urllib.parse import urlsplit
from urllib.request import url2pathname
import pandas as pd
#import policy
from rascrapetask import maxidom_urls

class FoldersProvider:
    def __init__(self, base_directory, postfix):
        self.folders_dict = dict()
        self.postfix = postfix
        self.base_directory = base_directory

    def nid_folder(self, sub_name):
        if sub_name not in self.folders_dict:
            new_sub = os.path.join(self.base_directory, sub_name+self.postfix)
            i = 0
            while os.path.exists(new_sub):
                i += 1
                new_sub = os.path.join(self.base_directory, sub_name+self.postfix+'-'+str(i))
            self.folders_dict[sub_name] = new_sub
        return self.folders_dict[sub_name]

"""
#test:
fp = FoldersProvider(base_directory=sys.path[0], postfix=datetime.now().strftime('-%Y-%m-%d_%H-%M'))

print(fp.nid_folder('ppp'))
print(fp.nid_folder('tt'))
print(fp.nid_folder('ppp'))
"""


def save_to_file(folders_provider, url, bytes):
    parsed = urlsplit(url)
    root_dir = folders_provider.nid_folder(url2pathname(parsed.netloc))
    part_dir, filename = os.path.split(url2pathname(parsed.path))
    if not filename:
        part_dir, filename = os.path.split(part_dir)
    dir = os.path.join(root_dir, part_dir.lstrip(os.path.sep))
    #print(f"root_dir: {root_dir}\npart_dir: {part_dir}\ndir: {dir}")
    os.makedirs(dir, exist_ok=True)
    if parsed.query:
        filename += '_' + url2pathname(parsed.query.replace('&', '_'))
    if parsed.fragment:
        filename += '_' + url2pathname(parsed.fragment)
    if not filename:
        filename = '_'
    i = 0
    full_filepath = os.path.join(dir, filename+'.html')
    while os.path.exists(full_filepath):
        i += 1
        full_filepath = os.path.join(dir, filename + '-' + str(i)+'.html')
    with open(full_filepath, 'wb') as fout:
        fout.write(bytes)
    #print('debug:', full_filepath)
    return os.path.relpath(full_filepath, (root_dir))  # os.path.dirname(root_dir)


def get_free_key(suggest_key, some_dict):
    ans = suggest_key
    i = 0
    while ans in some_dict:
        i += 1
        ans = suggest_key+'_'+str(i)
    return ans


def set_on_free_key(d, k, v):
    freekey = get_free_key(k, d)
    d[freekey] = v


def float_safe(s, valonex=0.0):
    if s is None:
        return valonex
    if isinstance(s, int) or isinstance(s, float):
        return s
    s = s.strip()
    if s == "":
        return valonex
    try:
        return float(s)
    except ValueError:
        try:
            return float(s.replace(',','.'))
        except ValueError:
            return valonex


class ArttexMaxiDomSpider(scrapy.Spider):
    name = "ArttexMaxiDomSpider"
    user_agent = "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; Googlebot/2.1; http://www.google.com/bot.html) Chrome/W.X.Y.Z‡ Safari/537.36"

    #def __init__(self, *args, **kwargs):
    #    logger = logging.getLogger('scrapy.core.engine')
    #    logger.setLevel(logging.INFO)
    #    super().__init__(*args, **kwargs)

    def start_requests( self ):
        self.ans = list()
        for url in self.start_urls:
            yield scrapy.Request( url = url,
                callback = self.process_list_page,
                cookies = {'MAXI_LOC_ID': '9'},  # 9 = Moscow
                cb_kwargs = dict(attempt=1))

    def process_list_page( self, response, attempt ):
        filename = save_to_file(self.folders_provider, response.url, response.body)
        if ((response.xpath('//div[@data-qa-error-block]')
                or response.status in [400, 403, 404, 408]
                # or not response.xpath('//h1[@data-qa-title]')
              )
            and (not isinstance(attempt,int) or attempt < 100)
          ):
            time.sleep(attempt * random.uniform(self.sleep_seconds_min, self.sleep_seconds_max))
            yield response.request.replace(dont_filter=True,
                cb_kwargs=dict(attempt=attempt+1))
        else:
            # """ """ """
            next_page_urls = response.xpath('//a[@id="navigation_3_next_page test"]/@href').getall()
            for next_page in next_page_urls:
                time.sleep(random.uniform(self.sleep_seconds_min, self.sleep_seconds_max))
                yield response.follow(url=next_page,
                    callback=self.process_list_page,
                    cb_kwargs=dict(attempt=1),
                  )
            # """ """ """
            list_title = ''.join(response.xpath('//h1//text()').getall())
            list_city = ''.join(response.xpath('(//a[contains(@class, "city")])[1]//text()').getall())
            for prodart in response.xpath('//article[@itemtype="http://schema.org/Product"]'):
                prodlink = prodart.xpath('.//a[@itemprop="name"]')
                product_page_url = prodlink.xpath('./@href').get()
                time.sleep(random.uniform(self.sleep_seconds_min, self.sleep_seconds_max))
                data_from_list_page = {'list_url': response.url,
                        'list_title': list_title,
                        'list_city': list_city,
                        'list_name': ''.join(prodlink.xpath('.//text()').getall()),
                        'list_price_text': ''.join(prodart.xpath('.//span[@class="price-list"]//span[@data-repid_price]//text()').getall()),
                        'list_price_attr': (prodart.xpath('.//span[@class="price-list"]//span[@data-repid_price]/@data-repid_price').get()),
                        'list_price_hidden': ''.join(prodart.xpath('.//span[@class="price-list"]//span[@itemprop="price"]//text()').getall()),
                        'list_price_older': ''.join(prodart.xpath('.//span[@class="price-older"]//text()').getall()),
                        'list_discount': ''.join(prodart.xpath('.//div[@class="b_discount_amount"]//text()').getall()),
                        'list_meas': ''.join(prodart.xpath('.//span[@class="measure"]//text()').getall()),
                      }
                for i, sku_top in enumerate(prodart.xpath('.//div[@itemprop="description"]//small[@class="sku"]'), start=1):
                    text = ''.join(sku_top.xpath('.//text()').getall())
                    if ':' in text:
                        k, v = text.split(':', 1)
                    else:
                        k = 'sku_top_'+str(i)
                        v = text
                    set_on_free_key(data_from_list_page, 'list_'+k.strip(), v.strip())
                for i, sku_bottom in enumerate(prodart.xpath('.//div[@class="small-bottom"]//small[@class="sku"]'), start=1):
                    text = ''.join(sku_bottom.xpath('.//text()').getall())
                    k = ('country' if i == 1 else 'weight' if i == 2 else 'sku_bottom_'+str(i))
                    v = text
                    set_on_free_key(data_from_list_page, 'list_'+k.strip(), v.strip())
                set_on_free_key(data_from_list_page, 'list_instock',
                    ''.join(prodart.xpath('.//span[contains(@class,"stock")]//text()').getall()))
                yield response.follow(url=product_page_url,
                    callback=self.process_product_page,
                    cb_kwargs=data_from_list_page)

    def process_product_page( self, response, **cb_kwargs ):
        filename = save_to_file(self.folders_provider, response.url, response.body)
        breadcrumbs = response.xpath('//ul[@itemtype="http://schema.org/BreadcrumbList"]//li//span[@itemprop="name"]//text()').getall()
        div_price = response.xpath('//div[@id="mnogo_prd_price"]')
        name = ''.join(response.xpath('//h2[@itemprop="name"]//text()').getall())
        city = ''.join(response.xpath('(//a[contains(@class, "city")])[1]//text()').getall())
        product = cb_kwargs
        product.update({'url': response.url, 'filename': filename,
            'city': city,
            'breadcrumbs': ' > '.join(breadcrumbs),
            'last_breadcrumb': (breadcrumbs[-2] if breadcrumbs and len(breadcrumbs) > 1
                else breadcrumbs[-1] if breadcrumbs else None),
            'name': name,
            'price': div_price.xpath('./@data-repid_price').get(),
            'price_text': ''.join(div_price.xpath('./text()').getall()),
            'price_old': ''.join(div_price.xpath('..//div[contains(@class,"price-old")]//text()').getall()),
            'price_discount': ''.join(div_price.xpath('..//div[contains(@class,"b_discount_amount")]//text()').getall()),
            'meas': ''.join(response.xpath('//*[@class="pack"]//text()').getall())
            })
        overview = response.xpath('//div[@id="overview"]')
        for wrap_param in overview.xpath('.//div[contains(@class,"wrap-param")]'):
            k = ''.join(wrap_param.css('span.param ::text').getall())
            v = ''.join(wrap_param.css('span.value ::text').getall())
            set_on_free_key(product, 'Описание '+k.strip().rstrip(':').strip(), v.strip())
        set_on_free_key(product, 'Описание топ', ' '.join(overview.xpath('.//p[1]//text()').getall()))
        set_on_free_key(product, 'Описание текст', ''.join(response.xpath('//section[@id="product-desc"]//p//text()').getall()))
        for characteristic in response.xpath('//section[@id="product-technicals"]'):
            for div in characteristic.xpath('./div'):  # class="tab-row"
                char_name = ''.join(div.xpath('./span[1]//text()').getall())
                char_value = ''.join(div.xpath('./span[2]//text()').getall())
                set_on_free_key(product, char_name, char_value)
        self.ans.append(product)

fp = FoldersProvider(
    base_directory = sys.path[0],
    postfix = dt.datetime.now().strftime('-%Y-%m-%d-%H%M')
  )

save_folder = fp.nid_folder(url2pathname(urlsplit(maxidom_urls[0]).netloc))

os.makedirs(save_folder)

logging.basicConfig(
    filename=os.path.join(save_folder,'scrapy.log'),
    encoding='utf-8',
    level=logging.DEBUG,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
  )

process = CrawlerProcess({
        'CONCURRENT_REQUESTS_PER_IP': 1,
        'RETRY_TIMES': 100,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 400, 403, 404, 408, 429],

        # Configure a delay for requests for the same website (default: 0)
        # See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
        # See also autothrottle settings and docs
        'DOWNLOAD_DELAY': 2,  # i.e. 30 ppm at most

        # Enable and configure the AutoThrottle extension (disabled by default)
        # See https://docs.scrapy.org/en/latest/topics/autothrottle.html
        'AUTOTHROTTLE_ENABLED': True,
        # The initial download delay
        'AUTOTHROTTLE_START_DELAY': 0.6,
        # The maximum download delay to be set in case of high latencies
        'AUTOTHROTTLE_MAX_DELAY': 60,
        # The average number of requests Scrapy should be sending in parallel to
        # each remote server
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 1.0,
        # Enable showing throttling stats for every response received:
        'AUTOTHROTTLE_DEBUG': True,
        ### 'ROTATING_PROXY_LIST_PATH': 'listofproxies.txt',
        ### 'ROTATING_PROXY_PAGE_RETRY_TIMES': 100,
        ### 'DOWNLOADER_MIDDLEWARES': {
        ###     # ...
        ###     'rotating_proxies.middlewares.RotatingProxyMiddleware': 800,
        ###     'rotating_proxies.middlewares.BanDetectionMiddleware': 800,
        ###     # ...
        ### },
        ### 'ROTATING_PROXY_BAN_POLICY': 'policy.MyBanPolicy',
        'COOKIES_DEBUG': True,
    })

logging.info('CrawlerProcess created')

process.crawl(ArttexMaxiDomSpider,
    folders_provider = fp,
    sleep_seconds_min = 0.0,
    sleep_seconds_max = 0.2,
    start_urls = maxidom_urls)

spider = min(process.crawlers).spider

try:
    process.start()
except Exception as e:
    logging.critical('Exception in process.start():', e)

filename = os.path.join(save_folder, 'output-unsorted.json')
with open(filename, 'w', encoding='utf-8') as fout:
    json.dump(spider.ans, fout, ensure_ascii=False, indent=2)
logging.info('Created '+filename)

try:
    spider.ans.sort(key=lambda p: (p.get('Артикул',''), p.get('Код товара',''), p.get('name',''), float_safe(p.get('price', '')), p.get('url', '')))
except Exception as e:
    logging.error('Exception in spider.ans.sort:', e)

filename = os.path.join(save_folder, 'output-sorted.json')
with open(filename, 'w', encoding='utf-8') as fout:
    json.dump(spider.ans, fout, ensure_ascii=False, indent=2)
logging.info('Created '+filename)

df = pd.DataFrame(spider.ans)
filename = os.path.join(save_folder, 'output.csv')
df.to_csv(filename)
logging.info('Created '+filename)

df['export_date'] = dt.datetime.now().date()

filename = os.path.join(save_folder, 'output-plus-date.csv')
df.to_csv(filename)
logging.info('Created '+filename)



