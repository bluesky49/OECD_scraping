from get_url_from_firstpage import Get_Url_From_FirstPage
import requests
from bs4 import BeautifulSoup
import urllib
import threading
import json
import pandasdmx
import time
import os
import sys

def datalink():
    urlsclass = Get_Url_From_FirstPage()
    urls = urlsclass.getUrl()
    
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    links = []
    url = urls[6]
    print(url)
    # if 'http://dx.doi.org/10.1787/d096af8d-en' in urls:
    #     print(urls.index('http://dx.doi.org/10.1787/d096af8d-en'))
    # for url in urls:
    result = requests.get(url, headers = headers)
    soup = BeautifulSoup(result.content,'html5lib')
    iframe = soup.find("iframe",id="previewFrame")
    csv = soup.find("span",text="CSV")
    if csv:
        aTagUrl = csv.find_parent('a')['href']
        result = requests.get("https://www.oecd-ilibrary.org" + aTagUrl,headers=headers)
        soup = BeautifulSoup(result.content,'html5lib')
        ultag = soup.find('ul')
        atags = ultag.find_all("a")
        aurls = [a['href'] for a in atags]
        for a in aurls:
            code = ""
            if 'DatasetCode' in a:
                code = a.split('DatasetCode=')[1]
            elif "DataSetCode" in a:
                code = a.split('DataSetCode=')[1]
            if code:
                datasetcode.append(code)
    if iframe:
        rr = "https:" + iframe['src']
        r = requests.get(rr, headers = headers)
        ressoup = BeautifulSoup(r.content,'html.parser')
        setcode = ressoup.find('script')
        codes = setcode.text
        ss = codes.split(" = ")
        rr = ss[1].replace("[","").replace("];","").replace("'",'"')
        lock.acquire()
        datasetcode.append(json.loads(rr)['dataSetCode'])
        print('in datalink append=', json.loads(rr)['dataSetCode'])
        lock.release()
    print(datasetcode)
    return links

def downloadCSV():
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.') 
    csv_path = os.path.join(script_dir, 'DATA_CSV')
    print(csv_path)
    oecd = pandasdmx.Request('OECD')
    while 1:
        lock.acquire()
        
        if len(datasetcode) > 0:
            code = datasetcode.pop(0)
            lock.release()
            print('in download code=', code)
            try:
                data_response = oecd.data(resource_id=code, key='all')
                print(data_response.data.series)
                df = data_response.write(data_response.data.series, parse_time=False)
                df.to_csv(csv_path + '\\' + code + '.csv', sep = ',')
                print('in download, completed to_csv')
            except:
                pass
        else:
            lock.release()
            time.sleep(0.01)


def main():
    datalink()
    
if __name__ == "__main__":
    lock = threading.Lock()
    datasetcode = []
    threading.Thread(target=downloadCSV).start()
    
    main()
