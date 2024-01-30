import json
from bs4 import BeautifulSoup
import requests
import pandas as pd
import json
import threading,queue,time,re
from tqdm import tqdm


def do_work():
    global progress_bar,all_data_list
    while True:
        time.sleep(1)
        if work_queue.empty():
            break
        sale_id,catlog_id,catlog_name = work_queue.get(timeout=10)
        page_no = 1
        while True:
            url = f'https://embed.salefinder.com.au/productlist/category/{sale_id}/?categoryId={catlog_id}&rows_per_page=300&page={page_no}&callback=jQuery172004288298288104708_1689721258202'
            # url = f'https://embed.salefinder.com.au/productlist/view/{sale_id}/?rows_per_page=300&page={page_no}&categoryId={catlog_id}&callback=jQuery17203931450602244311_1689278397435'
            # url = f'https://embed.salefinder.com.au/productlist/view//?saleGroup=0&preview=&rows_per_page=300&page=&callback=jQuery17203931450602244311_1689278397435'
            response = requests.get(url)
            if response.status_code != 200:
                print(response.status_code)
                time.sleep(5)
                continue
            start_index = response.text.index('(') + 1
            end_index = response.text.rindex(')')
            dict_string = response.text[start_index:end_index]
            data = json.loads(dict_string)
            soup = BeautifulSoup(data['content'],'lxml')
            all_product = soup.find_all('td',class_='sf-item')
            for product in all_product:
                details_tag = product.find('a',class_='sf-item-heading')
                try:
                    name = details_tag.text
                except:
                    continue
                half_url = details_tag['href']
                url = f'https://www.iga.com.au/catalogue/{half_url}'
                product_id = int(details_tag['data-itemid'])
                image_url = product.find('img')['src']
                try:
                    price = product.find('span',class_='sf-nowprice').text.strip()
                except:
                    price = None
                offer_text = product.find('span',class_='sf-regoption')
                if offer_text:
                    offer_text = offer_text.text.strip()
                comparative_text = product.find('p',class_='sf-comparativeText')
                if comparative_text:
                    comparative_text = comparative_text.text
                
                df1 = pd.DataFrame({'product_id':[product_id],
                                    'catlog_name':[catlog_name],
                                    'name':[name],
                                    'url':[url],
                                    'price':[price],
                                    'offer_text':[offer_text],
                                    'comparative_text':[comparative_text],
                                    'image_url':[image_url],
                                    'sale_id':[sale_id],
                                    'catlog_id':[catlog_id]
                                    })
                all_data_list.append(df1)
            next_page = soup.find('a',class_='page-numbers')
            if next_page:
                print('page no')
                page_no +=1
            else:
                progress_bar.update(1)
                break
                # df = pd.concat([df,df1])



store_df = pd.read_csv('sale_id.csv')
sale_ids = store_df['sale_id'].to_list()
sale_ids = list(set(sale_ids))

all_data_list = []
work_queue = queue.Queue()

for sale_id in sale_ids:
    sale_id = int(sale_id)
    url = f'https://embed.salefinder.com.au/productlist/view/{sale_id}/?rows_per_page=300&page=1&callback=jQuery17203931450602244311_1689278397435'
    response = requests.get(url)
    start_index = response.text.index('(') + 1
    end_index = response.text.rindex(')')
    dict_string = response.text[start_index:end_index]
    data = json.loads(dict_string)
    soup = BeautifulSoup(data['content'],'lxml')
    for catlog in soup.find_all('a',class_='sf-navcategory-link'):
        match = re.search(r'categoryId=(\d+)', catlog['href'])
        catlog_id = int(match.group(1))
        catlog_name = catlog.text
        work_queue.put([sale_id,catlog_id,catlog_name])
# for index,row in store_df.iterrows():
#     work_queue.put(row)

total_size = work_queue.qsize()
threads = []
for _ in range(20):
    thread = threading.Thread(target=do_work)
    threads.append(thread)
    thread.start()
    
df = pd.DataFrame()
progress_bar = tqdm(total=total_size, unit='iteration')
while progress_bar.n != total_size:
    time.sleep(1)
    progress_bar.set_postfix({'Queue Size': work_queue.qsize()})
# Close the progress bar
progress_bar.close()
for thread in threads:
    thread.join()
print('Please wait while saving data')
df = pd.concat(all_data_list)
df.to_excel('finaldata.xlsx',index=False)
print('data saved')