import requests
from api_keys import api_key1,X_RapidAPI_Key1


querystring = {"api_key":api_key1}

headers = {
	"X-RapidAPI-Key": X_RapidAPI_Key1,
	"X-RapidAPI-Host": "big-data-amazon.p.rapidapi.com"
}
# read from random-tems.txt to get different amazon products
with open('random-items.txt','r') as file:
    items = file.read()
 
items = items.split('\n')
with open('api-data-list.json','a') as file:
	for i in range(401,700):
		id = items[i]
		print(f"{i+1}.id={id}\t",end ='')
		url = f"https://big-data-amazon.p.rapidapi.com/products/{id}"
		response = requests.request("GET", url, headers=headers, params=querystring)
		if (response.status_code ==200):
			file.write(response.text)
			file.write(",")
			file.write("\n")
		print(f"status:{response.status_code}")

