from cgitb import html
from bs4 import BeautifulSoup
import requests

url = 'https://imfeelingprimey.com/'
response = requests.get(url)




soup = BeautifulSoup(response.text,'html.parser')



classses = soup.find_all("span", class_="prod-title")

with open('random-items.txt','w') as file:
    for item in classses:
        item_link = item.find_parent('a').get("href")
        id= item_link.split("/")[5]
        file.write(id+"\n")
