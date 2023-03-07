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
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.remote.webelement import WebElement
import undetected_chromedriver
from rafilesysutils import FoldersProvider, save_to_file
from radictutils import get_free_key, set_on_free_key
from raconvutils import float_safe
from rascrapetask import *

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

collected_items = []

#with webdriver.Chrome() as driver:
with undetected_chromedriver.Chrome() as driver:

    #open start page
    driver.get(leroymerlin_first_page)
    button_region_accept = get_by_xp(driver, '//button[@data-qa-region-accept]', 2)
    if button_region_accept:
        button_region_accept.click()
        time.sleep(2)

    #open catalogue (this fails if the screen isn't wide enough)
    button_catalogue = get_by_xp(driver, '//button[@data-qa-catalogue-button or @data-qa-header-catalogue-button or contains(@data-qa, "catalogue-button")]')
    button_catalogue.click()
    time.sleep(2)

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
        time.sleep(1)
        button_sort = get_by_xp(driver, '//button[@aria-label="Сортировка товаров"]')
        button_sort.click()
        time.sleep(1)
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
        time.sleep(1)
        logging.info(f'{driver.current_url} - {driver.name} - {driver.title}')
        a_next_page = True
        while a_next_page:
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
                        'prod_full_url': a_prod.get_attribute('href'),
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
                time.sleep(2)
                a_next_page = True

            #a_next_page = False  #short-circuit for debugging

        time.sleep(2)

    #driver.close()

#write output files:

filename = os.path.join(save_folder, 'output-unsorted.json')
with open(filename, 'w', encoding='utf-8') as fout:
    json.dump(collected_items, fout, ensure_ascii=False, indent=2)
logging.info('Created '+filename)

try:
    collected_items.sort(key=lambda p: (p.get('prod_list_sku', ''), p.get('prod_list_name', ''), float_safe(p.get('prod_list_price', '')), p.get('prod_dom_url', '')))
except Exception as e:
    logging.error('Exception in collected_items.sort:', e)

df = pd.DataFrame(collected_items)
filename = os.path.join(save_folder, 'output-sorted-without-date.csv')
df.to_csv(filename)
logging.info('Created '+filename)

df['export_date'] = dt.datetime.now().date()

filename = os.path.join(save_folder, 'output-sorted-with-date.csv')
df.to_csv(filename)
logging.info('Created '+filename)




