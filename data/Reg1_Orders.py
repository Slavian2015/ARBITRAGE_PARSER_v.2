import requests
import json

oid = 214582644

response = requests.get('https://btc-alpha.com/api/v1/order/{}/'.format(oid))
obj = json.loads(response.text)
print(obj)


