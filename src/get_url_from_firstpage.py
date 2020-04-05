import requests
from bs4 import BeautifulSoup
class Get_Url_From_FirstPage:
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    response = requests.get('https://data.oecd.org/search-api/?hf=20&b=0&r=%2Bf%2Ftype%2Fdatasets&r=%2Bf%2Flanguage%2Fen&l=en&sl=sl_dp&sc=enabled%3Atrue%2Cautomatically_correct%3Atrue&target=st_dp',headers=headers)
    soup = BeautifulSoup(response.content,'html5lib')

    def __init__(self):
        self.urls = []
        
    def getUrl(self):
        meta = Get_Url_From_FirstPage.soup.findAll('metastring',attrs={'name':'value'})
        for i in meta:
            if i.previous_sibling['name'] == 'url' or i.previous_sibling['name'] == 'ispartof_serial_doi':
                if i.text not in self.urls:
                    self.urls.append(i.text)
        return self.urls
if __name__ == "__main__":
    g = Get_Url_From_FirstPage()
    g.getUrl()
    print("successfully get", len(g.urls), "urls")