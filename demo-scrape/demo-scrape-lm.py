import sys
import os
import time
import datetime as dt
import logging
import json
import random
from urllib.parse import urlsplit
from urllib.request import url2pathname
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait as WDW
#from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.remote.webelement import WebElement
import undetected_chromedriver
from rafilesysutils import FoldersProvider, save_to_file
from radictutils import get_free_key, set_on_free_key
from raconvutils import float_safe
from rascrapetask import *  # if you need the complete solution, please contact me directly https://t.me/oradim
from selenium.webdriver.remote.remote_connection import LOGGER as selenium_logger
from urllib3.connectionpool import log as urllib_logger

"""
ra, 2023: This script uses selenium to extract lists of products (including prices) from :param leroymerlin_first_page.
The result is exported to csv files using pandas.
"""

#TODO: extract function that processes a single product page
#TODO: extract function to process list pages

SHORT_PAUSE = 0.3
LONG_PAUSE = 0.6

qauth_script_xp = '/html/head/script[@src="/__qrator/qauth_utm_v2.js"]'
href_next_page_xp = '//a[@data-qa-pagination-item="right"]/@href'
href_prev_page_xp = '//a[@data-qa-pagination-item="left"]/@href'
h1_title_xp = '//h1[@data-qa-title]'
div_error_xp = '//div[@data-qa-error-block]'
uc_502_xp = '/html/body/uc-app/uc-502-error-page'

def nav_to_url(dr, url):
    dr.get(url)
    attempt = 0
    while attempt < 10:
        attempt += 1
        element = None
        try:
            element = WDW(dr, 45).until(
                EC.any_of(
                    EC.presence_of_element_located((By.XPATH, href_next_page_xp)),
                    EC.presence_of_element_located((By.XPATH, href_prev_page_xp)),
                    EC.presence_of_element_located((By.XPATH, h1_title_xp)),
                    EC.presence_of_element_located((By.XPATH, div_error_xp)),
                    EC.presence_of_element_located((By.XPATH, uc_502_xp)),
                )
            )
        except TimeoutException:
            # добавить qauth_script_xp
            element = dr.find_element(By.XPATH, qauth_script_xp)
            if element is None:
                logging.critical('TimeoutException on waiting for page from url %s: %s', url, ex)
        if isinstance(element, WebElement):
            if ('div' == element.tag_name
                and element.get_dom_attribute('data-qa-error-block') is not None
                ):
                time.sleep(50)
                dr.refresh()
                time.sleep(10)
                continue
            if ('h1' == element.tag_name):
                logging.debug(element)
                return
        time.sleep(30)

def get_by_xp(driver, xpath, timeout_seconds=5):
    locator = (By.XPATH, xpath)
    try:
        return WDW(driver, timeout_seconds).until(
                EC.presence_of_element_located(locator)
        )
    except TimeoutException:
        return None


dt_postfix = dt.datetime.now().strftime('-%Y-%m-%d-%H%M')

fp = FoldersProvider(
    base_directory = sys.path[0],
    postfix = dt_postfix
  )

save_folder = fp.nid_folder(url2pathname(urlsplit(leroymerlin_first_page).netloc))

os.makedirs(save_folder)

selenium_logger.setLevel(logging.INFO)  # Set the threshold for selenium
urllib_logger.setLevel(logging.INFO)  # Set the threshold for urllib3

logging.basicConfig(
    filename=os.path.join(save_folder, f'demo-scrape-lm{dt_postfix}.log'),
    encoding='utf-8',
    level=logging.DEBUG,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
  )

rootLogger = logging.getLogger()
consoleHandler = logging.StreamHandler()
logFormatter = logging.Formatter("%(asctime)s [%(name)s] [%(levelname)-5.5s]  %(message)s")
consoleHandler.setFormatter(logFormatter)
consoleHandler.setLevel(logging.DEBUG)
consoleHandler.addFilter(lambda r: not r.name.startswith('selenium'))
rootLogger.addHandler(consoleHandler )

logging.info('Got ready to navigate the target site.')

def calc_percent(a_part, a_total):
    if a_total:
        return round(100*a_part/a_total)
    else:
        return 100

def navigate_to_url(driver, url, retry_count=15, sleep_seconds_min=150, sleep_seconds_max=660):
    for retry in range(retry_count):
        try:
            driver.get(url)
            time.sleep(SHORT_PAUSE)
            div_out_of_stock = None
            try:
                div_out_of_stock = driver.find_element(By.XPATH, '//div[@data-qa="out-of-stock-lable"]')
            except NoSuchElementException:
                div_out_of_stock = None
            if div_out_of_stock is None:  # на странице нет признака "Товар закончился"
                return
            else:
                data_qa_attr = div_out_of_stock.get_attribute('data-qa')
                logging.debug(f'Received {div_out_of_stock.text} ({data_qa_attr}) in response from {url}. Sleeping for {sleep_seconds_min} seconds before retrying...')
                time.sleep(sleep_seconds_min)
        except TimeoutException as ex:
            sleep_seconds = random.uniform(sleep_seconds_min, sleep_seconds_max)
            logging.debug(str(ex)+f' -- Sleeping for {sleep_seconds} seconds before retrying...')
            time.sleep(sleep_seconds)
    raise TimeoutException(f'Failed to navigate to {url} after {retry_count} attempts :(')

def scrape_data_from_product_pages(driver, collected_items):
    logging.info('Ready to process individual pages.')
    start_time = time.time()
    done_pages_count = 0
    failed_pages_count = 0
    total_pages_count = len(collected_items)
    for product in collected_items:
        product_page_process_success = False
        retry = 0
        MAX_RETRY = 15
        while not product_page_process_success and retry < MAX_RETRY:
            retry += 1
            try:
                navigate_to_url(driver, product['prod_abs_url'])
                time.sleep(SHORT_PAUSE * random.uniform(1, 5))

                filename = save_to_file(fp, driver.current_url, driver.page_source)

                breadcrumbs = driver.find_elements(By.XPATH, '//span[@data-testid="breadcrumbItemName"]')
                div_title = driver.find_element(By.XPATH, '//div[@data-testid="product-title_mf-pdp"]')
                sku_text = div_title.find_element(By.XPATH, './div/div/span').text
                sku_meta = driver.find_element(By.XPATH, '//meta[@itemprop="sku"]').get_attribute('content')
                name = div_title.find_element(By.XPATH, './/h1//span').text
                city = driver.find_element(By.XPATH, '//button[@data-qa="header-regions-button"]').text
                div_availability = driver.find_element(By.XPATH, '//div[@data-testid="availability_mf-pdp"]')

                #product2 = product.copy()
                product2 = {
                    'url': driver.current_url,
                    'filename': filename,
                    'city': city,
                    'breadcrumbs': ' > '.join([b.text for b in breadcrumbs]),
                    'last_breadcrumb': (breadcrumbs[-1].text if breadcrumbs else None),
                    'sku_text': sku_text,
                    'sku': sku_meta,
                    'name': name,
                    'availability': div_availability.text,
                    }

                for slot in ('price', 'fract', 'currency', 'unit'):
                    product2[slot+'_view'] = ''

                for k_if_discount in ('price_new', 'unit_new', 'price_old', 'unit_old'):
                    product2[k_if_discount] = ''

                showcase_price_view = None  # обычная цена
                try:
                    showcase_price_view = driver.find_element(By.XPATH, '//showcase-price-view[@slot="primary-price"]')
                except NoSuchElementException:
                    showcase_price_view = None

                if showcase_price_view is not None:
                    for slot in ('price', 'fract', 'currency', 'unit'):
                        current_slot = None
                        try:
                            current_slot = showcase_price_view.find_element(By.XPATH, f'.//span[@slot="{slot}"]')
                        except NoSuchElementException:
                            current_slot = None

                        if current_slot is not None:
                            product2[slot+'_view'] = current_slot.text

                    div_prices_mf_pdp = showcase_price_view.find_element(By.XPATH, '..')

                else:  # если нет обычной цены -- то искать цену со скидкой
                    div_oph_price = None
                    #ra, 2023-03-11: решил исключить ситуации, когда нет ни обычной цены, ни цены со скидкой
                    # если нет никакой цены -- то что-то "не так" -- и пусть find_element выбросит исключение,
                    # (и данная страница будет запрошена повторно на следующей итерации внешнего цикла while) 
                    #try:
                    div_oph_price = driver.find_element(By.XPATH, '//div[@data-qa="oph-price"]')
                    #except NoSuchElementException:
                    #    div_oph_price = None

                    if div_oph_price is not None:
                        product2['price_new'] = div_oph_price.find_element(By.XPATH, './following-sibling::div[1]/span[1]').text
                        product2['unit_new'] = div_oph_price.find_element(By.XPATH, './following-sibling::div[1]/span[2]').text
                        product2['price_old'] = div_oph_price.find_element(By.XPATH, './following-sibling::div[2]/span[1]').text
                        product2['unit_old'] = div_oph_price.find_element(By.XPATH, './following-sibling::div[2]/span[2]').text
                        div_prices_mf_pdp = div_oph_price.find_element(By.XPATH, '../..')
                    else:
                        div_prices_mf_pdp = driver.find_element(By.XPATH, '//div[@data-testid="prices_mf-pdp"]')
                    
                for itemprop in ('price', 'priceCurrency'):
                    product2[itemprop] = div_prices_mf_pdp.find_element(By.XPATH, f'./div//meta[@itemprop="{itemprop}"]').get_attribute('content')
                for dl in driver.find_elements(By.XPATH, '//dl'):
                    for div in dl.find_elements(By.XPATH, './/div'):
                        char_name = div.find_element(By.XPATH, './/dt').text
                        char_value = div.find_element(By.XPATH, './/dd').text
                        set_on_free_key(product2, char_name, char_value)

                product_page_process_success = True  # to exit from loop

            except (StaleElementReferenceException, NoSuchElementException) as ex:
                
                product_page_process_success = False  # to retry reading the current product page
                logging.debug(ex)
                #if retry >= MAX_RETRY:
                #    raise
                time.sleep(LONG_PAUSE)
                if retry < MAX_RETRY:
                    logging.debug(f'Will retry this page again: {product["prod_abs_url"]}')


        if product_page_process_success:
                
            product.update(product2)
            
            done_pages_count += 1
            if done_pages_count % 100 == 0:
                logging.info(f'{calc_percent(done_pages_count, total_pages_count)}% - Done scraping data from {done_pages_count} individual pages out of {total_pages_count} products total in {dt.timedelta(seconds=time.time()-start_time)}.')
        else:
            failed_pages_count += 1
            logging.error(f'Failed to scrape data from this page: {product["prod_abs_url"]} after {retry} attempt(s).')

    if done_pages_count % 100:
        logging.info(f'{calc_percent(done_pages_count, total_pages_count)}% - Done scraping data from {done_pages_count} individual page(s) out of {total_pages_count} products total in {dt.timedelta(seconds=time.time()-start_time)}.')

    if failed_pages_count:
        logging.info(f'{failed_pages_count} failed page(s).')

collected_items = []

#with webdriver.Chrome() as driver:
with undetected_chromedriver.Chrome() as driver:

    #open start page
    driver.get(leroymerlin_first_page)
    button_region_accept = get_by_xp(driver, '//button[@data-qa-region-accept]', 2)
    if button_region_accept:
        button_region_accept.click()
        time.sleep(LONG_PAUSE)

    #open catalogue (this fails if the screen isn't wide enough)
    button_catalogue = get_by_xp(driver, '//button[@data-qa-catalogue-button or @data-qa-header-catalogue-button or contains(@data-qa, "catalogue-button")]')
    button_catalogue.click()
    time.sleep(LONG_PAUSE)

    cat_links = {}

    for category in leroymerlin_categories:

        a_category = get_by_xp(driver, f'//a[contains(.,"{category}")]')
        #a_category.click()
        # do not follow the links immediately, just save the urls;
        # first relative, then absolute:
        cat_links[category] = (a_category.get_dom_attribute('href'), a_category.get_attribute('href'))

    logging.debug(cat_links)

    for category in leroymerlin_categories:
        driver.get(cat_links[category][1])  # use absolute url
        time.sleep(SHORT_PAUSE)
        button_sort = get_by_xp(driver, '//button[@aria-label="Сортировка товаров"]')
        button_sort.click()
        time.sleep(SHORT_PAUSE)
        label_price_asc = button_sort.find_element(By.XPATH, './/label[contains(., "Цена по возрастанию")]')
        price_asc_selected = label_price_asc.get_attribute('data-qa-dropdown-selected')
        #logging.debug(f'{label_price_asc.text} - {price_asc_selected} - {type(price_asc_selected)}')
        if price_asc_selected not in ('false', False):
            #logging.debug(f'true == {price_asc_selected}')
            button_sort.click()
        else:
            #logging.debug(f'false == {price_asc_selected}')
            #label_price_asc.find_element(By.TAG_NAME, 'span').click()
            label_price_asc.click()
        time.sleep(SHORT_PAUSE)
        logging.info(f'{driver.current_url} - {driver.title}')
        a_next_page = True
        page_number = 0
        while a_next_page and (leroymerlin_limit_list_pages_per_category is None or page_number < leroymerlin_limit_list_pages_per_category):
            page_number += 1
            a_next_page = get_by_xp(driver, '//a[@data-qa-pagination-item="right"]')
            a_prev_page = get_by_xp(driver, '//a[@data-qa-pagination-item="left"]', timeout_seconds = 1)

            save_to_file(fp, driver.current_url, driver.page_source)

            list_title = driver.find_element(By.XPATH, '//h1[@data-qa-title]').text
            list_city = driver.find_element(By.XPATH, '//button[@data-qa="header-regions-button"]').text
            for a_prod in driver.find_elements(By.XPATH, '//a[@data-qa="product-name"]'):
                #time.sleep(random.uniform(self.sleep_seconds_min, self.sleep_seconds_max))
                div_prices = a_prod.find_element(By.XPATH, '../..//div[@data-qa="product-primary-price"] | ../..//div[@data-qa="product-old-new-price"]')
                prod_list_old_price = ''
                prod_list_old_unit = ''
                if div_prices.get_attribute('data-qa') == 'product-primary-price':
                    prod_list_price = div_prices.find_element(By.XPATH, './p[1]').text
                    prod_list_unit = div_prices.find_element(By.XPATH, './p[2]').text
                else:
                    prod_list_price = div_prices.find_element(By.XPATH, './div[2]/span[1]').text
                    prod_list_unit = div_prices.find_element(By.XPATH, './div[2]/span[2]').text
                    prod_list_old_price = div_prices.find_element(By.XPATH, './div[3]/span[1]').text
                    prod_list_old_unit = div_prices.find_element(By.XPATH, './div[3]/span[2]').text
                data_from_list_page = {
                        'list_url': driver.current_url,
                        'list_title': list_title,
                        'list_city': list_city,
                        'prod_dom_url': a_prod.get_dom_attribute('href'),
                        'prod_abs_url': a_prod.get_attribute('href'),
                        'prod_list_name': a_prod.text,
                        'prod_list_sku': a_prod.find_element(By.XPATH, '..//span[@data-qa-product-article]').text,
                        'prod_list_price': prod_list_price,
                        'prod_list_unit': prod_list_unit,
                        'prod_list_old_price': prod_list_old_price,
                        'prod_list_old_unit': prod_list_old_unit,
                      }
                collected_items.append(data_from_list_page)
            if a_next_page:
                a_next_page.click()
                time.sleep(LONG_PAUSE)
                a_next_page = True


        time.sleep(LONG_PAUSE)

    filename = os.path.join(save_folder, 'output-01-from-list-pages-only.json')
    with open(filename, 'w', encoding='utf-8') as fout:
        json.dump(collected_items, fout, ensure_ascii=False, indent=2)
    logging.info('Created '+filename)

    scrape_data_from_product_pages(driver, collected_items)

    filename = os.path.join(save_folder, 'output-02-incl-individ-prod-pages.json')
    with open(filename, 'w', encoding='utf-8') as fout:
        json.dump(collected_items, fout, ensure_ascii=False, indent=2)
    logging.info('Created '+filename)

    #driver.close()

#write output files:

try:
    collected_items.sort(key=lambda p: (p.get('prod_list_sku', ''), p.get('prod_list_name', ''), float_safe(p.get('prod_list_price', '')), p.get('prod_dom_url', '')))
except Exception as e:
    logging.error('Exception in collected_items.sort:', e)

df = pd.DataFrame(collected_items)
filename = os.path.join(save_folder, 'output-03-sorted-without-date.csv')
df.to_csv(filename)
logging.info('Created '+filename)

df['export_date'] = dt.datetime.now().date()

filename = os.path.join(save_folder, 'output-04-sorted-with-date.csv')
df.to_csv(filename)
logging.info('Created '+filename)




