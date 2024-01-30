import json
from bs4 import BeautifulSoup
import requests
import pandas as pd
import json
import threading,queue,time
from tqdm import tqdm
from urllib.parse import parse_qs, urlparse


def do_work():
    global progress_bar,all_data_list
    session = requests.Session()
    session.headers.update(headers)
    while True:
        time.sleep(1)
        if work_queue.empty():
            break
        row = work_queue.get(timeout=10)
        storeId = row['storeId']
        storeName = row['storeName']
        address = row['address']
        suburb = row['suburb']
        state = row['state']
        phone = row['phone']
        latitude = row['latitude']
        longitude = row['longitude']
        postcode = row['postcode']
        page_no = 1
        while True:
            url = f'https://embed.salefinder.com.au/catalogues/view/183/?format=json&locationId={storeId}&order=oldestfirst&callback=jQuery1720854929639409425_1689360483639'
            response = requests.get(url,headers=headers)
            if response.status_code != 200:
                print(response.status_code)
                time.sleep(5)
                continue
            start_index = response.text.index('(') + 1
            end_index = response.text.rindex(')')
            dict_string = response.text[start_index:end_index]
            data = json.loads(dict_string)
            soup = BeautifulSoup(data['content'],'lxml')
            for catlog_element in soup.find_all('div',class_='sale-image-cell'):
                catlog_href = catlog_element.find('a')['href']
                url_parts = urlparse(catlog_href)
                query_params = parse_qs(url_parts.fragment)
                sale_id = query_params.get('saleId', [''])[0]
            # try:
            #     url_parts = urlparse(data['redirect'])
            #     query_params = parse_qs(url_parts.fragment)
            #     sale_id = query_params.get('saleId', [''])[0]
            # except:
            #     progress_bar.update(1)
            #     break
                # sale_id = None
                df1 = pd.DataFrame({'sale_id':[sale_id],
                                    'storeId':[storeId],
                                    'storeName':[storeName],
                                    'address':[address],
                                    'suburb':[suburb],
                                    'state':[state],
                                    'postcode':[postcode],
                                    'phone':[phone],
                                    'latitude':[latitude],
                                    'longitude':[longitude],
                                    })
                all_data_list.append(df1)
            progress_bar.update(1)
            break



store_df = pd.read_csv('storelist.csv')
headers = {
    'User-Agent':'Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36'
}

all_data_list = []
work_queue = queue.Queue()

for index,row in store_df.iterrows():
    work_queue.put(row)
    # break

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
df.to_csv('sale_id.csv',index=False)
print('data saved')