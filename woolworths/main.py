import requests
import pandas as pd
import json
import threading,queue,time
from tqdm import tqdm
import re
from bs4 import BeautifulSoup


def find_additional_information(price_promotional_information_text,price_promotional_information_html):
    was_price_additional_information = None
    a_tag_additional_information = None
    if price_promotional_information_text:
        try:
            if 'was' in price_promotional_information_text or 'Was' in price_promotional_information_text :
                pattern = r'\$([\d.]+)'
                match = re.search(pattern, price_promotional_information_text)
                if match:
                    was_price_additional_information = float(match.group(1))
        except:
            pass
                
    if price_promotional_information_html:
        try:
            soup = BeautifulSoup(price_promotional_information_html,'lxml')
            if soup.find('a'):
                href = soup.find('a')['href']
                a_tag_additional_information = f'https://www.woolworths.com.au{href}'
        except:
            pass
            
    return was_price_additional_information,a_tag_additional_information

    
headers = {
    'user-agent': 'Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36',
}

def do_work():
    global progress_bar,all_data_list
    json_data = {
        'categoryId': '1_24E1176',
        'pageNumber': 1,
        'pageSize': 36,
        'sortType': 'Name',
        'url': '/shop/browse/winter',
        'location': '/shop/browse/winter',
        'formatObject': '{"name":"Winter"}',
        'isSpecial': None,
        'isBundle': False,
        'isMobile': True,
        'filters': [],
        'token': '',
        'gpBoost': 0,
        'isHideUnavailableProducts': None,
        'categoryVersion': 'v2',
    }
    while True:
        time.sleep(1)
        if work_queue.empty():
            break
        catlog = work_queue.get(timeout=10)
        category_id = catlog['NodeId']
        url_name = catlog['UrlFriendlyName']
        if 'specials' in url_name or 'front' in url_name:
            progress_bar.update(1)
            continue
        catlog_name = catlog['Description']
        url = f'/shop/browse/{url_name}'
        format_object = '{"name":"'+catlog_name+'"}'
        json_data['categoryId'] = category_id
        json_data['url'] = url
        json_data['location'] = url
        json_data['formatObject'] = format_object
        # print(name)
        page_no = 1
        error = 0
        while True:
            print(f'{catlog_name} : Page No {page_no}')
            time.sleep(1)
            json_data['pageNumber'] = page_no
            try:
                response = session.post('https://www.woolworths.com.au/apis/ui/browse/category', json=json_data,timeout=10)
            except:
                error+=1
                time.sleep(5)
                if error == 3:
                    progress_bar.update(1)
                    # print('Cookies Expired')
                    break
                continue
            if response.status_code == 200:
                if len(response.json()['Bundles']) == 0:
                    progress_bar.update(1)
                    break
                for result in response.json()['Bundles']:
                    detials = result['Products'][0]
                    stockcode = detials['Stockcode']
                    name = detials['Name']
                    display_name = detials['DisplayName']
                    barcode = detials['Barcode']
                    cup_price = detials['CupPrice']
                    price_per_amount = detials['CupString']
                    # was_price = detials['InstoreCupPrice']
                    was_price = detials['WasPrice']
                    price = detials['Price']
                    if price == was_price:
                        was_price = None
                    source = detials['Source']
                    is_in_stock = detials['IsInStock']
                    package_size = detials['PackageSize']
                    is_pm_delivery = detials['IsPmDelivery']
                    is_available = detials['IsAvailable']
                    instore_is_available = detials['InstoreIsAvailable']
                    is_purchasable = detials['IsPurchasable']
                    brand = detials['Brand']
                    catlog_result = detials['AdditionalAttributes']
                    category = catlog_result['piesdepartmentnamesjson']
                    sub_category = catlog_result['piescategorynamesjson']
                    sub_sub_category = catlog_result['piessubcategorynamesjson']
                    small_image_file = detials['SmallImageFile']
                    medium_image_file = detials['MediumImageFile']
                    large_image_file = detials['LargeImageFile']
                    url_friendly = detials['UrlFriendlyName']
                    url = f'https://www.woolworths.com.au/shop/productdetails/{stockcode}/{url_friendly}'
                    rating = detials['Rating']
                    rating_average = rating['Average']
                    addtional_information = detials['CentreTag']
                    price_promotional_information_text = addtional_information['TagContentText']
                    price_promotional_information_html = addtional_information['TagContent']
                    was_price_additional_information,a_tag_additional_information = find_additional_information(price_promotional_information_text,price_promotional_information_html)
                    df1 = pd.DataFrame({
                        'barcode': [barcode],
                        'stockcode': [stockcode],
                        'name': [name],
                        'display_name': [display_name],
                        'cup_price': [cup_price],
                        'price_per_amount':[price_per_amount],
                        'was_price': [was_price],
                        'price': [price],
                        'source': [source],
                        'is_in_stock': [is_in_stock],
                        'package_size': [package_size],
                        'is_pm_delivery': [is_pm_delivery],
                        'is_available': [is_available],
                        'instore_is_available': [instore_is_available],
                        'is_purchasable': [is_purchasable],
                        'brand': [brand],
                        'category': [category],
                        'sub_category': [sub_category],
                        'sub_sub_category': [sub_sub_category],
                        'url_friendly': [url_friendly],
                        'small_image_file':[small_image_file],
                        'medium_image_file':[medium_image_file],
                        'large_image_file':[large_image_file],
                        'url': [url],
                        'rating_average': [rating_average],
                        'price_promotional_information_text':[price_promotional_information_text],
                        'price_promotional_information_html':[price_promotional_information_html],
                        'was_price_additional_information':[was_price_additional_information],
                        'a_tag_additional_information':[a_tag_additional_information]
                        })
                    all_data_list.append(df1)
                    error=0
                    # all_data.put(df1)
                # progress_bar.update(1)
                # break
                page_no+=1
            elif response.status_code == 403:
                error+=1
                time.sleep(5)
                if error == 3:
                    progress_bar.update(1)
                    print('permission denied')
                    print(name)
                    break
            else:
                error+=1
                time.sleep(5)
                if error == 3:
                    progress_bar.update(1)
                    print('error')
                    print(response.status_code)
                    print(response.text)
                    print(name)
                    break

all_data_list = []
headers = {
    'User-Agent':'Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36'
} 
response = requests.get('https://www.woolworths.com.au/api/ui/v2/bootstrap',headers=headers)
catlog_list = response.json()['ListTopLevelPiesCategories']['Categories']

session = requests.Session()
session.headers.update(headers)
session.get('https://www.woolworths.com.au/')
work_queue = queue.Queue()
for catlog in catlog_list:
    work_queue.put(catlog)
total_size = work_queue.qsize()
threads = []
for _ in range(10):
    thread = threading.Thread(target=do_work)
    threads.append(thread)
    thread.start()


df = pd.DataFrame()
progress_bar = tqdm(total=total_size, unit='iteration')
while progress_bar.n != total_size:
    time.sleep(1)
    progress_bar.set_postfix({'Queue Size': work_queue.qsize()})
    if threading.active_count() == 1:
        break
# Close the progress bar
progress_bar.close()
for thread in threads:
    thread.join()
print('Please wait while saving data')


df = pd.concat(all_data_list)
df.to_csv('finaldata.csv',index=False)
print('data saved')