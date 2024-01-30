import requests,time,threading,pytz,queue,os,json,sys,datetime
import pandas as pd
from tqdm import tqdm
sys.path.append('../')
from all_function import *

website = 'coles'
column = ['website','store_id', 'product_id', 'catlog', 'location_name', 'brand_name', 'address', 'suburb', 'state', 'phone', 'postcode', 'latitude', 'longitude', 'url', 'product_brand', 'product_full_name', 'name', 'description', 'size', 'availability', 'current_price', 'old_price', 'save_amount', 'comparable', 'promotion_description', 'promotion_type', 'image_url']

def do_work():
    global progress_bar,data_not_present,data_changed,sql_data
    while True:
        try:
            time.sleep(1)
            if work_queue.empty():
                break
            store_id,location_name,brand_name,address,suburb,state,phone,postcode,latitude,longitude,catlog = work_queue.get(timeout=10)
            store_id = str(store_id)
            catlog = str(catlog)
            page_no = 1
            cookies = {
                'fulfillmentStoreId': store_id,
                'ageGateVerified':'true'
            }
            params = {
                'slug':catlog,
                'page':'1'
            }
            count = 0
            while True:
                time.sleep(1)
                params['page'] = page_no
                try:
                    response = requests.get(f'https://www.coles.com.au/_next/data/{api_id}/en/browse/{catlog}.json',cookies= cookies,params=params,timeout=15)
                except:
                    time.sleep(20)
                if response.status_code == 200:
                    noOfResults = response.json()['pageProps']['searchResults']['noOfResults']
                    if noOfResults == 0:
                        progress_bar.update(1)
                        work_queue.task_done()
                        print(f'finished Stored Id : {store_id} catlog :{catlog} pageno {page_no-1} count {count}')
                        break
                    for result in response.json()['pageProps']['searchResults']['results']:
                        if result['_type']== 'PRODUCT':
                            adId = result['adId']
                            if adId is not None:
                                continue
                            product_id = result['id']
                            name = result['name']
                            product_brand = result['brand']
                            size = result['size']
                            product_full_name = f'{product_brand} {name} | {size}'
                            description = result['description']
                            availability = result['availability']
                            pricing = result['pricing']
                            if pricing is not None:
                                promotion_type = pricing.get('promotionType')
                                current_price = pricing.get('now')
                                old_price = pricing.get('was')
                                save_amount = pricing.get('saveAmount')
                                promotion_description = pricing.get('promotionDescription')
                                comparable = pricing.get('comparable')
                            else:
                                promotion_type,current_price,old_price,save_amount,promotion_description,comparable = None, None, None, None, None, None
                            image = result['imageUris'][0]['uri']
                            image_url = f'https://productimages.coles.com.au/productimages{image}'
                            url = f'https://www.coles.com.au/product/{product_id}' 
                            val = (website,store_id, product_id, catlog, location_name, brand_name, address, suburb, state, phone, postcode, latitude, longitude, url, product_brand, product_full_name, name, description, size, availability, current_price, old_price, save_amount, comparable, promotion_description, promotion_type, image_url)
                            if sql_data.get(f'{product_id}_{catlog}'):
                                if sql_data[f'{product_id}_{catlog}'] != val:               # not same
                                    new_val = val+(val[2],val[3])       # add id and catlog
                                    data_changed.append(new_val)
                            else:
                                data_not_present.append(val)               # not present
                            new_val = val+(date_time,)  
                            all_data.append(new_val)
                            count+=1
                        # break
                    page_no+=1
                elif response.status_code == 404:
                    print('The website api is updated please run update_api.py')
                    break
                else:
                    print('sleep')
                    time.sleep(20)
        except Exception as e:
            print('Main error',e)
            break


def add_work_queue():
    global store_list
    store_list = pd.read_csv('store list.csv')
    for index,row in store_list.iterrows():
        store_id = row['storeId']
        location_name = row['locationName']
        brand_name = row['brandName']
        address = row['address']
        suburb = row['suburb']
        state = row['state']
        phone = row['phone']
        postcode = row['postcode']
        latitude = row['latitude']
        longitude = row['longitude']
        for catlog in catlog_list:
            work_queue.put([store_id,location_name,brand_name,address,suburb,state,phone,postcode,latitude,longitude,catlog])
    
def get_api_id():
    filename = "api_id.json"
    with open(filename, "r") as file:
        json_data = json.load(file)
    api_id = json_data["api_id"]
    return api_id



api_id = get_api_id()
response = requests.get(f'https://www.coles.com.au/_next/data/{api_id}/en/browse.json')
catlog_result = response.json()['pageProps']['initialState']['bffApi']['queries']['getProductCategories({"storeId":"0584"})']['data']['catalogGroupView']
catlog_list = []
for catlog in catlog_result:
    catlog_list.append(catlog['seoToken'])

work_queue = queue.Queue()

sql_data = fetch_sql_data(website,column)
data_changed = []
data_not_present = []
all_data = [] 
# gmt_plus_6 = pytz.timezone('Etc/GMT+6')
# date_time = datetime.datetime.now(gmt_plus_6)
australia_sydney = pytz.timezone('Australia/Sydney')
date_time = datetime.datetime.now(australia_sydney)

add_work_queue()
threads = []
for _ in range(15):
    thread = threading.Thread(target=do_work)
    threads.append(thread)
    thread.start()
    
total_size = work_queue.qsize()
progress_bar = tqdm(total=total_size, unit='iteration')
while progress_bar.n != total_size:
    time.sleep(1)
    progress_bar.set_postfix({'Queue Size': work_queue.qsize()})

# Close the progress bar
progress_bar.close()
print('Please wait while saving data')
for thread in threads:
    thread.join()

update_mysql(website,data_changed,data_not_present,all_data,column)
print('completed')