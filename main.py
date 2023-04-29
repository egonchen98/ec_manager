import re
import requests
from bs4 import BeautifulSoup
url = "http://i.tq121.com.cn/j/weather2015/bluesky/c_7d.js?20230412_104308"
resp = requests.get(url)
resp.encoding = "utf-8"
print(resp.text)
