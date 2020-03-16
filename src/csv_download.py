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

def get_datacode():
    urlsclass = Get_Url_From_FirstPage()
    urls = urlsclass.getUrl()
    links = []
    for url in urls:
        print(url)
        result = requests.get(url, headers = headers)
        soup = BeautifulSoup(result.content,'html5lib')
        iframe = soup.find("iframe",id="previewFrame")
        csvs = soup.find_all("span",text="CSV")
        intros = soup.find_all(class_="intro-item")
        js_archives = soup.find_all(class_="js-archives")
        
        if csvs:
            getDatasetCode_by_csv(csvs)
        if iframe:
            getDatasetCode_by_ifram(iframe)
        else:
            if intros:
                getDatasetCode_by_intro(intros) 
            if js_archives:
                getDatasetCode_by_archives(js_archives)
        flag = 1
        
    return links

def getDatasetCode_by_archives(js_archives):
    aurls= ['https://www.oecd-ilibrary.org' + i['href'] for i in js_archives]
    for i in aurls:
        result = requests.get(i,headers=headers)
        soup = BeautifulSoup(result.content, 'html5lib')
        iframe = soup.find('iframe',id="previewFrame")
        if iframe:
            getDatasetCode_by_ifram(iframe)
        else:
            continue
def getDatasetCode_by_ifram(iframe):
    rr = "https:" + iframe['src']
    r = requests.get(rr, headers = headers)
    ressoup = BeautifulSoup(r.content,'html.parser')
    setcode = ressoup.find('script')
    if setcode:
        codes = setcode.text
        ss = codes.split(" = ")
        rr = ss[1].replace("[","").replace("];","").replace("'",'"')
        lock.acquire()
        datasetcode.append(json.loads(rr)['dataSetCode'])
        print('in datalink append=', json.loads(rr)['dataSetCode'])
        lock.release()

def getDatasetCode_by_csv(csvs):
    for csv in csvs:
        aTagUrl = csv.find_parent('a')['href']
        if not aTagUrl:
            continue
        result = requests.get("https://www.oecd-ilibrary.org" + aTagUrl,headers=headers)
        s = BeautifulSoup(result.content,'html5lib')
        oecdStat = s.find('a',text='OECD.Stat')
        if not oecdStat:
            continue
        csvlink = oecdStat.find_parent('p').find_all('a')[1]['href']
        result = requests.get(csvlink, headers)
        soup = BeautifulSoup(result.content,'html5lib')
        ultag = soup.find('ul')
        if not ultag:
            continue
        atags = ultag.find_all("a")
        if not atags:
            continue
        aurls = [a['href'] for a in atags]
        for a in aurls:
            code = ""
            if 'DatasetCode' in a:
                code = a.split('DatasetCode=')[1]
            elif "DataSetCode" in a:
                code = a.split('DataSetCode=')[1]
            if code and code not in datasetcode:
                lock.acquire()
                datasetcode.append(code)
                lock.release()

def getDatasetCode_by_intro(intros):
    aurls = []
    for i in intros:
        if i.name == 'a':
            aurls.append('https://www.oecd-ilibrary.org' + i['href'])
        if i.name == 'p':
            aurls.append('https://www.oecd-ilibrary.org' + i.find('a')['href'])
    for i in aurls:
        result = requests.get(i, headers = headers)
        soup = BeautifulSoup(result.content,'html5lib')
        iframe = soup.find('iframe',id="previewFrame")
        csvs = soup.find_all("span",text="CSV")
        if iframe:
            getDatasetCode_by_ifram(iframe)
        if csvs:
            getDatasetCode_by_csv(csvs)
def downloadCSV():
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.') 
    csv_path = os.path.join(script_dir, 'DATA_CSV')
    oecd = pandasdmx.Request('OECD')
    while 1:
        lock.acquire()
        
        if len(datasetcode) > 0:
            code = datasetcode.pop(0)
            lock.release()
            print('in download code=', code)
            try:
                data_response = oecd.data(resource_id=code, key='all')
                df = data_response.write(data_response.data.series, parse_time=False)
                df.to_csv(csv_path + '\\' + code + '.csv', sep = ',')
                print('in download, completed to_csv')
            except:
                pass
        else:
            lock.release()
            time.sleep(0.01)


def main():
    get_datacode()
    
if __name__ == "__main__":
    lock = threading.Lock()
    datasetcode = []
    flag = 1
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    thread = threading.Thread(target=downloadCSV)
    thread.start()
    
    main()
