from pyecharts import Geo
import xlrd


data = xlrd.open_workbook('gc.xlsx')

table2 = data.sheets()[0]
nrows = table2.nrows  # 读取第一个工作表的行数
count = 1
city = []

for i in range(1, nrows):
    row = table2.row_values(i)
    v = tuple((row[1], row[2]))
    city.append(v)

city.pop(-1)
print(city)

geo = Geo("全国工厂分布", "data from gc", title_color="#fff",
          title_pos="center", width=1000,
          height=600, background_color='#404a59')
attr, value = geo.cast(city)
geo.add("", attr, value, visual_range=[0, 200], maptype='china', visual_text_color="#fff",
        symbol_size=10, is_visualmap=True)

data = xlrd.open_workbook('t.xlsx')
table2 = data.sheets()[0]
nrows = table2.nrows  # 读取第一个工作表的行数
count = 1
city = []

for i in range(2, nrows):
    row = table2.row_values(i)
    v = tuple((row[0], row[2]))
    city.append(v)

attr, value = geo.cast(city)
geo.add("", attr, value, visual_range=[0, 200], maptype='china', visual_text_color="#1ff",
        symbol_size=10, is_visualmap=True)

geo.render("全国工厂分布.html")  # 生成html文件
