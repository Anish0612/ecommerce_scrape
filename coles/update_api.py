import undetected_chromedriver as uc
import json

options = uc.ChromeOptions()
options.add_argument(
    "user-agent = Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36")
options.add_argument("--headless")
options.add_argument("--disable-extensions")
options.add_argument("--allow-running-insecure-content")
driver = uc.Chrome(options=options, use_subprocess=True,version_main=114)

driver.get('https://www.coles.com.au/browse/meat-seafood')
network = driver.execute_script("return window.performance.getEntries();")
for res in network:
    name = res['name']
    if '_buildManifest.js' in name:
        api_id = name.split('/')[-2]
        
if api_id is None:
    print("some thing problem can't able to fetch api Id" )
json_data = {"api_id": api_id}
filename = "api_id.json"
with open(filename, "w") as file:
    json.dump(json_data, file)
driver.close()
driver.quit()