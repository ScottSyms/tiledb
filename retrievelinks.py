import requests
from bs4 import BeautifulSoup

def fetchurls(x):
    # Fetch webpage
    url = 'https://coast.noaa.gov/htdata/CMSP/AISDataHandler/20' + f"{x:02d}" + '/index.html'  # Replace with your target URL
    baseurl='/'.join(url.split('/')[:-1])
    response = requests.get(url)
    html = response.content

    # Parse HTML and extract links
    soup = BeautifulSoup(html, 'html.parser')
    links = soup.find_all('a')

    # Extract href attributes
    urls = [link.get('href') for link in links if link.get('href') is not None]
    return [baseurl +'/' + file for file in urls if file.endswith('.zip')]


if __name__ == '__main__':
    links=[]
    for x in range(9, 23):
        links=links +fetchurls(x)

    f=open('links.txt','w')
    for i in links:
        f.write(i+'\n')
    f.close()
