import requests
import pandas as pd
import json
import threading,queue,time
from tqdm import tqdm


def do_work():
    while True:
        time.sleep(1)
        if work_queue.empty():
            break
        postcode = work_queue.get(timeout=10)
        while True:
            try:
                url = f'https://embed.salefinder.com.au/location/search/183/?sensitivity=5&noStoreSuffix=1&withStoreInfo=1&callback=jQuery172020614143698531895_1689192636225&query={postcode}&_=1689192837675'
                response = requests.get(url)
                if response.status_code == 200:
                    progress_bar.update(1)
                    # print(count)
                    # completed_postcode.append(postcode)
                    start_index = response.text.index('(') + 1
                    end_index = response.text.rindex(')')
                    dict_string = response.text[start_index:end_index]
                    data = json.loads(dict_string)
                    for store_data in data['result']:
                        df1 = pd.DataFrame([store_data])
                        all_data_list.append(df1)
                    break
                        # store_df = pd.concat([store_df,df1])
                else:
                    print('error')
                    break
            except:
                time.sleep(3)
    
headers = {
    'User-Agent':'Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36'
}
session = requests.Session()
session.headers.update(headers)
df = pd.read_csv('australian_postcodes.csv')
postcode_list = df['postcode'].to_list()
postcode_list = list(set(postcode_list))
work_queue = queue.Queue()
all_data_list = []
for postcode in postcode_list:
    work_queue.put(postcode)

total_size = work_queue.qsize()
threads = []
for _ in range(50):
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
df = df.drop_duplicates()
df.to_csv('storelist.csv',index=False)
print('data saved')