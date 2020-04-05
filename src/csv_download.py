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
from selenium.common.exceptions import NoSuchElementException
def get_datacode():
    urlsclass = Get_Url_From_FirstPage()
    urls = urlsclass.getUrl()
    links = []
    for url in urls:
        try:
            result = requests.get(url, headers = headers)
            soup = BeautifulSoup(result.content,'html5lib')
            iframe = soup.find("iframe",id="previewFrame")
            csvs = soup.find_all("span",text="CSV")
            intros = soup.find_all(class_="intro-item")
            js_archives = soup.find_all(class_="js-archives")
            datas = soup.find_all('span', class_="name-action")
            
            if csvs:
                getDatasetCode_by_csv(csvs)
            if iframe:
                getDatasetCode_by_ifram(iframe)
            else:
                if intros:
                    getDatasetCode_by_intro(intros) 
                if js_archives:
                    getDatasetCode_by_archives(js_archives)
            if datas:
                getDatasetCode_by_data(datas)
            flag = 1
        except:
            continue    
            
    return links

def getDatasetCode_by_archives(js_archives):
    aurls= ['https://www.oecd-ilibrary.org' + i['href'] for i in js_archives]
    for i in aurls:
        try:
            result = requests.get(i,headers=headers)
            soup = BeautifulSoup(result.content, 'html5lib')
            iframe = soup.find('iframe',id="previewFrame")
            datas = soup.find_all('span', class_="name-action")
            if iframe:
                getDatasetCode_by_ifram(iframe)
            else:
                if datas:
                    getDatasetCode_by_data(datas)
        except:
            continue
def getDatasetCode_by_ifram(iframe):
    rr = "https:" + iframe['src']
    r = requests.get(rr, headers = headers)
    ressoup = BeautifulSoup(r.content,'html.parser')
    title = ressoup.find("title").text.replace("\r","").replace("\n","").replace("\t","")
    setcode = ressoup.find('script')
    if setcode:
        codes = setcode.text
        ss = codes.split(" = ")
        rr = ss[1].replace("[","").replace("];","").replace("'",'"')
        
        
        js = json.loads(rr)['dataSetCode']
        if js not in datasetcode and js != "":
            lock.acquire()
            datasetcode.append(js)
            if title not in filename:
                filename.append(title)
            lock.release()

def getDatasetCode_by_csv(csvs):
    for csv in csvs:
        try:
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
                r = requests.get(a,headers = headers)
                s = BeautifulSoup(r.content,'html5lib')
                title = s.find("title").text.replace("\n","").replace("\t","").replace("\r","")
                if 'DatasetCode' in a:
                    code = a.split('DatasetCode=')[1]
                elif "DataSetCode" in a:
                    code = a.split('DataSetCode=')[1]
                if code and code not in datasetcode and code !="":
                    lock.acquire()
                    datasetcode.append(code)
                    filename.append(title)
                    lock.release()
        except:
            continue

def getDatasetCode_by_data(datas):
    dtlinks = []
    for data in datas:
        if "DATA" in data.text:
            dtlinks.append(data)
    if not dtlinks:
        return 
    for data in dtlinks:
        try:
            res = requests.get("https://oecd-ilibrary.org" + data.find_parent('a')['href'],headers = headers)
            s = BeautifulSoup(res.content, 'html5lib')
            scripts = s.find_all('script')
            title = s.find("title").text.replace('\n','').replace('\t','')
            if not scripts:
                return
            for i in scripts:
                if 'dataLayer' in i.text and 'dataSetCode' in i.text:
                    w = eval(i.text.replace("dataLayer = [","").replace("];",""))
                    if w['dataSetCode'] and w['dataSetCode'] not in datasetcode and w['dataSetCode'] != "":
                        lock.acquire()
                        datasetcode.append(w['dataSetCode'])
                        filename.append(title)
                        lock.release()
        except:
            continue

def getDatasetCode_by_intro(intros):
    aurls = []
    for i in intros:
        if i.name == 'a':
            aurls.append('https://www.oecd-ilibrary.org' + i['href'])
        if i.name == 'p':
            aurls.append('https://www.oecd-ilibrary.org' + i.find('a')['href'])
    for i in aurls:
        try:
            result = requests.get(i, headers = headers)
            soup = BeautifulSoup(result.content,'html5lib')
            iframe = soup.find('iframe',id="previewFrame")
            csvs = soup.find_all("span",text="CSV")
            datas = soup.find_all('span', class_="name-action")
            if datas:
                getDatasetCode_by_data(datas)
            if iframe:
                getDatasetCode_by_ifram(iframe)
            if csvs:
                getDatasetCode_by_csv(csvs)
        except:
            continue
        
def downloadCSV():
    try:
        script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.') 
        csv_path = os.path.join(script_dir, 'DATA_CSV')
        oecd = pandasdmx.Request('OECD')
        while 1:
            lock.acquire()
            if len(datasetcode) > 0:
                code = datasetcode.pop(0)
                title = filename.pop(0)
                lock.release()
                print('in datasetcode=', code, "length of datasetcode ", len(datasetcode))
                try:
                    data_response = oecd.data(resource_id=code, key='all')
                    df = data_response.write(data_response.data.series, parse_time=False)
                    s = str(title)
                    while s[-1]==".":
    	                s = s[:-1]
                    df.to_csv(csv_path + '\\' + s + '.csv', sep = ',')
                    print('in download, completed to_csv',"datacode=",code)
                except:
                    pass
            else:
                lock.release()
                time.sleep(0.01)
    except:
        pass


def main():
    get_datacode()
    
if __name__ == "__main__":
    lock = threading.Lock()
    datasetcode = []
    filename = []
    flag = 1
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    thread = threading.Thread(target=downloadCSV)
    thread.start()
    main()
    exit()
