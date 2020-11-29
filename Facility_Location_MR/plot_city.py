from pyecharts import Geo
import urllib
import json
import jsonpath
from urllib import request
from urllib.request import urlopen, quote


def get_location_coordinate(location_name):
    api_url = 'http://api.map.baidu.com/geocoding/v3/?address='
    api_url = f'{api_url}{quote(location_name)}&output=json&ak=y5kWUwfyvAXoixuOFAl3UYL3eUgCpsRK'
    result = urlopen(api_url)
    result = json.loads(result.read().decode())['result']['location']
    return result['lng'], result['lat']

url = "http://www.lagou.com/lbs/getAllCitySearchLabels.json"
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"}
request = urllib.request.Request(url, headers=headers)
response = urllib.request.urlopen(request)
html = response.read()

jsonobj = json.loads(html)
city_list = jsonpath.jsonpath(jsonobj, "$..name")

print(city_list)

except_list = ['博尔塔拉', '贺州', '海外', '台湾', '香港']
city = []
nrows = len(city_list)
# for i in range(nrows):
#     if city_list[i] not in except_list:
#         v = tuple((city_list[i], 100))
#         city.append(v)
count = 0
geo_cities_coords = {}
for location in city_list:
    if location not in except_list:
        count += 1
        print(count)
        print(location)
        v = tuple((location, 100))
        city.append(v)
        lat_long = get_location_coordinate(location)
        geo_cities_coords[location] = list(lat_long)
print(geo_cities_coords)

print(city)

geo = Geo("全国城市分布", "data from gc", title_color="#fff",
          title_pos="center", width=1000,
          height=600, background_color='#404a59')
attr, value = geo.cast(city)
geo.add("", attr, value, visual_range=[0, 200], maptype='china', visual_text_color="#fff",
        geo_cities_coords=geo_cities_coords,
        symbol_size=10, is_visualmap=True)

geo.render("全国城市分布.html")  # 生成html文件
