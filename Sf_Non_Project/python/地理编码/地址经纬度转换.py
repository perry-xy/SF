# 高德API地理编码与逆编码
import requests
import pandas as pd

key = "947f05dfcc702af644a2e1f9e2f0adbd"
base = 'https://restapi.amap.com/v3/geocode/regeo'


def get_data(file_path):
    """获取待解析的地址"""
    data_posi = pd.read_excel(file_path)
    return data_posi


def get_lnglat(file_path):
    data_posi = get_data(file_path)
    """地址转经纬度"""
    lng = []
    lat = []
    for i in range(0, len(data_posi)):
        url = "http://restapi.amap.com/v3/geocode/geo?key=%s&address=%s" % (key, data_posi["地址"][i])
        add = requests.get(url)
        info = add.json()
        if info['status'] == '0':
            lng.append("请求失败")
            lat.append("请求失败")
            print("请求失败" + ":" + data_posi["地址"][i])
        elif info['geocodes'] == []:
            lng.append("无经纬度信息")
            lat.append("无经纬度信息")
            print("无经纬度信息" + ":" + data_posi["地址"][i])
        else:
            lng.append(info['geocodes'][0]['location'].split(',')[0])
            lat.append(info['geocodes'][0]['location'].split(',')[1])
    lng_s = pd.Series(lng)
    lat_s = pd.Series(lat)
    data_posi['lng'] = lng_s
    data_posi['lat'] = lat_s
    data_posi.to_excel(file_path, index=False)


def get_address(file_path):
    data_addr = get_data(file_path)
    """经纬度转地址"""
    addr = []
    country = []
    province = []
    city = []
    district = []
    township = []
    street = []
    for i in range(0, len(data_addr)):
        parameters = {'output': 'json', 'location': data_addr["经纬度"][i], 'key': key, 'extensions': 'all'}
        response = requests.get(base, parameters)
        answer = response.json()
        addr.append(answer['regeocode']['formatted_address'])
        country.append(answer['regeocode']['addressComponent']['country'])
        province.append(answer['regeocode']['addressComponent']['province'])
        city.append(answer['regeocode']['addressComponent']['city'])
        district.append(answer['regeocode']['addressComponent']['district'])
        township.append(answer['regeocode']['addressComponent']['township'])
        street.append(answer['regeocode']['addressComponent']['streetNumber']['street'])
    addr_s = pd.Series(addr)
    country_s = pd.Series(country)
    province_s = pd.Series(province)
    city_s = pd.Series(city)
    district_s = pd.Series(district)
    township_s = pd.Series(township)
    street_s = pd.Series(street)
    data_addr['地址'], data_addr["国家"], data_addr["省份"], data_addr["城市"], data_addr["行政区"], data_addr["社区/乡镇街道"], \
    data_addr["道路街道名称"] = addr_s, country_s, province_s, city_s, district_s, township_s, street_s
    data_addr.to_excel(file_path, index=False)


get_lnglat(u"D:/code/Python/file/地理编码.xlsx")
get_address(u"D:/code/Python/file/逆地理编码.xlsx")