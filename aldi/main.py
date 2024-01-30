import requests,os
from bs4 import BeautifulSoup
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
website = 'aldi'


def is_available(element):
    try:
        value_name = element.text.strip()
    except:
        value_name = None
    return value_name


def scrape_catlog():
    url = 'https://www.aldi.com.au/en/groceries/'
    response = requests.get(url,verify=False)
    soup = BeautifulSoup(response.content, 'lxml')
    result = soup.find('article',id='main-content')
    catlog_urls = []
    for a_tag in result.find_all('a'):
        url = a_tag['href']
        if 'groceries' in url:
            response = requests.get(url,verify=False)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'lxml')
                all_product = soup.find_all('a',class_='box--wrapper ym-gl ym-g25')
                if all_product:                    
                    catlog_urls.append(url)
                else:                         # empty
                    result = soup.find('article',id='main-content')
                    for element in result.find_all('a'):
                        element_url = element['href']
                        if url in element_url:
                            catlog_urls.append(element_url)
            else:
                print('error',response.status_code)
    return catlog_urls



df = pd.DataFrame()
all_data_mysql = []
catlog_urls = scrape_catlog()
print('started')
data_changed = []
data_not_present = []
count = 0
for url in catlog_urls:
    count+=1
    print(f'{count}/{len(catlog_urls)}')
    response = requests.get(url,verify=False)
    filtered_list = list(filter(lambda x: x != '', url.split('/')))[4:]
    catlog = 'groceries'
    if len(filtered_list) == 1:
        catlog1 = filtered_list[0]
        catlog2 = None
    else:
        catlog1 = filtered_list[0]
        catlog2 = filtered_list[1]
    soup = BeautifulSoup(response.content, 'lxml')
    all_product = soup.find_all('a',class_='box--wrapper ym-gl ym-g25')
    for product in all_product:
        url = product['href']
        name = product.find('div',class_='box--description--header').text.strip()
        price_element = product.find('div',class_='box--price')
        old_price = price_element.find('span',class_='box--former-price')
        old_price = is_available(old_price)
        box_amount = price_element.find('span',class_='box--amount')
        box_amount = is_available(box_amount)
        price_int = price_element.find('span',class_='box--value')
        price_int = is_available(price_int)
        price_dec = price_element.find('span',class_='box--decimal')
        price_dec = is_available(price_dec)
        if price_dec is not None:
            price = price_int+price_dec
        else:
            price = price_int
        if price is not None and '$' not in price:
            price = price + 'c'
        saving = price_element.find('span',class_='box--saveing')
        saving = is_available(saving)
        base_price = price_element.find('span',class_='box--baseprice')
        base_price = is_available(base_price)
        image_url = product.find('img')['src']        
        df1 = pd.DataFrame({'name':[name],
                            'catlog':[catlog],
                            'catlog1':[catlog1],
                            'catlog2':[catlog2],
                            'price':[price],
                            'old_price':[old_price],
                            'saving':[saving],
                            'base_price':[base_price],
                            'box_amount':[box_amount],
                            'url':[url],
                            'image_url':[image_url],
                            })
        df = pd.concat([df,df1])
    
df.to_excel('finaldata.xlsx',index=False)
