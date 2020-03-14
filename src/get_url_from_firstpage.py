import requests
import xml.etree.ElementTree as ET
class Get_Url_From_FirstPage:
    response = requests.get('https://data.oecd.org/search-api/?hf=764&b=0&r=%2Bf%2Ftype%2Fdatasets&r=%2Bf%2Flanguage%2Fen&s=desc(document_publicationdate)&l=en&sl=sl_dp&sc=enabled%3Atrue%2Cautomatically_correct%3Atrue&target=st_dp')
    root = ET.fromstring(response.content)
    
    def __init__(self):
        self.urls = []
        
    def getUrl(self):
        for child in Get_Url_From_FirstPage.root.iter('{exa:com.exalead.search.v10}Meta'):
            if child.attrib['name'] == 'url' or child.attrib['name'] == 'publicurl':
                for ch in child.iter("{exa:com.exalead.search.v10}MetaString"):
                    if ch.text not in self.urls:
                        self.urls.append(ch.text)
        return self.urls
