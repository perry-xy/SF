##水平堆叠条形图
from pyecharts import options as opts
from pyecharts.charts import Bar
from pyecharts.faker import Faker

c = (
    Bar()
    .add_xaxis(Faker.choose())
    .add_yaxis("商家A", Faker.values(), stack="stack1")
    .add_yaxis("商家B", Faker.values(), stack="stack1")
    .add_yaxis("商家C", Faker.values())
    .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    .set_global_opts(title_opts=opts.TitleOpts(title="Bar-堆叠数据（部分）"))
    .reversal_axis()
    .render("bar_stack1.html")
)

##物料专配省份热力图—pyecharts==1.7.1
from pyecharts import options as opts
from pyecharts.charts import Map
import pandas as pd

RDC_Province=pd.read_excel(u"D:/物料专配/1.0/中间文件/RDC配送流向流量.xlsx",sheet_name="Sheet8",usecols=["目的省份","求和项:2020年预估重量(KG）"])
RDC_Province.drop(index=[31],inplace=True)
RDC_Province.columns=["province","weight"]
province=list(RDC_Province['province'])
weight=list(RDC_Province['weight'])

#图例分段
piece=[{'max':500000},
      {'min':600000,'max':1500000},
      {'min':1600000,'max':2600000},
      {'min':3000000,'max':6000000},
      {'min':8000000,'max':16000000},
      {'min':30000000}]

#定义Map()
c=(
    Map(init_opts=opts.InitOpts(width="2000px",height="900px")) ##初始化配置项
    .add("2020各省份需求量",[list(z) for z in zip(province, weight)],maptype="china")
    .set_global_opts(
        title_opts=opts.TitleOpts(title="物料专配需求分布"),
        visualmap_opts=opts.VisualMapOpts(is_piecewise=True,pieces=piece)
        )##全局配置项
    .render("物料专配需求分布.html")
)

##仓网覆盖连线—pyecharts==1.7.1
import pandas as pd
from pyecharts import options as opts
from pyecharts.charts import Geo
from pyecharts.globals import ChartType, SymbolType
from pyecharts.faker import Faker
from pyecharts.globals import GeoType

path=u"D:/Code/Facility_Location_WL/C_Network_10.csv"
data_pyfugai=pd.read_csv(open(path))

#获取RDC仓位置和城市经纬度坐标点
RDC_cords={data_pyfugai.iloc[i]['RDC_NAME']:[data_pyfugai.iloc[i]['RDC_LGT'],data_pyfugai.iloc[i]['RDC_LAT']] for i in range(len(data_pyfugai))} ##会自动去重
City_cords = {data_pyfugai.iloc[i]['CUSTOMER_NAME']:[data_pyfugai.iloc[i]['CUSTOMER_LGT'],data_pyfugai.iloc[i]['CUSTOMER_LAT']] for i in range(len(data_pyfugai))}
#所有的覆盖流向
data_flow=data_pyfugai[["RDC_NAME","CUSTOMER_NAME"]]

#所有的RDC仓名放入列表
RDC_city=list(RDC_cords.keys())
rdc_name=[]
rdc_value=[]
for i in range(0,len(RDC_city)):
    x=RDC_city[i].split("市")
    y=x[0]+'仓'
    rdc_name.append(y)
    rdc_value.append(1)

#初始化地图
c=Geo(init_opts=opts.InitOpts(width='1500px',height='700px'))
c.add_schema(
        maptype="china",
        itemstyle_opts=opts.ItemStyleOpts(color="#F0CA00", border_color="#111"))

#所有城市、RDC仓的坐标加进地图中
City=list(City_cords.keys())
JW=list(City_cords.values())
RDC_JW=list(RDC_cords.values())
for i in range(0,len(City)):
    c.add_coordinate(City[i], JW[i][0], JW[i][1])
for i in range(0,len(RDC_city)):
    c.add_coordinate(rdc_name[i], RDC_JW[i][0], RDC_JW[i][1])

#地图中加RDC仓的点
c.add("RDC仓",
    [list(z) for z in zip(rdc_name,rdc_value)],
        type_="scatter",label_opts=opts.LabelOpts(formatter="{b}",color='red',font_size=16,font_weight='bold')
        ,color='#3481B8',symbol='pin',
    # 标记图形形状,circle，pin，rect，diamon，roundRect，arrow，triangle
          symbol_size=16
         )
#加需求点
#去除RDC仓
xqd_city=[]
for i in range(0,len(City)):
    if City[i] not in list(RDC_cords.keys()):
        xqd_city.append(City[i])
city_value=[]
for i in range(0,len(xqd_city)):
    city_value.append(1)
c.add("需求点",
    [list(z) for z in zip(xqd_city,city_value)],
        type_="scatter",label_opts=opts.LabelOpts(is_show=False)
        ,color='blue',blur_size=1,symbol_size=5)

#循环添加每一个覆盖
colors=["#f948f7","#d1c667","#c76813","#2c12ef","#231439","#65f2d6","#bdef0a","#a21c68","#e38105","#42d1bd"]
for i in range(0,len(RDC_city)):
    data_rdc=data_flow[data_flow['RDC_NAME']==RDC_city[i]]
    flow=[]
    for j in range(0,len(data_rdc)):
        flow.append((data_rdc.iloc[j,0],data_rdc.iloc[j,1]))
    c.add(series_name=rdc_name[i],data_pair=flow,type_=GeoType.LINES,
          linestyle_opts=opts.LineStyleOpts(width=0.8,color=colors[i]),
          label_opts=opts.LabelOpts(is_show=False),
          ##trail_length=0,
          is_polyline=True
)
c.set_global_opts(title_opts=opts.TitleOpts(title="十仓仓网覆盖"))
c.render("py十仓仓网覆盖.html")

##漏斗图-pyecharts==1.7.1
from pyecharts import options as opts
from pyecharts.charts import Funnel
import pandas as pd

data_zhl=pd.read_excel(u"D:/code/Python/file/转化率作图.xlsx")
attr = data_zhl.环节
values = data_zhl.总体转化率

c=Funnel(init_opts=opts.InitOpts(width='1500px',height='700px'))
c.add('example',is_selected=False,
      data_pair=[list(z) for z in zip(attr,values)],
      label_opts=opts.LabelOpts(is_show=True,position='right',formatter='{c}'+'%')
      )
c.set_global_opts(title_opts=opts.TitleOpts(title="Funnel-基本示例"))
c.render('转化漏斗.html')

##供应商覆盖连线—pyecharts==1.7.1
import pandas as pd
from pyecharts import options as opts
from pyecharts.charts import Geo
from pyecharts.globals import ChartType, SymbolType
from pyecharts.faker import Faker
from pyecharts.globals import GeoType

# 数据读入
path_gysrdc = u"D:/物料专配/3.0/0923物料数据处理/0925结果/CDC_RDC_Network_7.csv"
path_gys = "d:/物料专配/3.0/0923物料数据处理/供应商.xlsx"
gys_rdc = pd.read_csv(open(path_gysrdc))
gys_total = pd.read_excel(path_gys, sheet_name='factory')
gis = pd.read_excel(path_gys, sheet_name='GIS')
gys_rdc[['CDC_Name', 'RDC_Name']] = gys_rdc[['CDC_Name', 'RDC_Name']].astype(int).astype(str)
gys_total['城市代码'] = gys_total['城市代码'].astype(int).astype(str)
gis['城市代码'] = gis['城市代码'].astype(int).astype(str)

# 取出未使用的供应商
gys_unuse_df = pd.merge(left=gys_total, right=gys_rdc, how='left', left_on='城市代码', right_on='CDC_Name')
gys_unuse = gys_unuse_df[list(gys_unuse_df['CDC_Name'].isna())].drop_duplicates()['城市代码']

# 初始化地图
c = Geo(init_opts=opts.InitOpts(width='1500px', height='700px'))
c.add_schema(
    maptype="china",
    itemstyle_opts=opts.ItemStyleOpts(color="#F0CA00", border_color="#111"))

# CDC位置加入地图
CDC_cords = {gys_total.iloc[i]['城市代码']: (gys_total.iloc[i]['CDC_LGT'], gys_total.iloc[i]['CDC_LAT']) for i in
             range(len(gys_total))}  ##会自动去重
for key, value in CDC_cords.items():
    c.add_coordinate(key, value[0], value[1])

# 使用的CDC加入地图
c.add("被选择的供应商",
      [[use_cdc_city, 1] for use_cdc_city in gys_rdc['CDC_Name'].drop_duplicates()],
      type_="scatter", label_opts=None,
      color='#3481B8', symbol_size=8)

# 未使用的CDC加入地图
c.add("未被选择的供应商",
      [[unuse_cdc_city, 1] for unuse_cdc_city in list(gys_unuse)],
      type_="scatter", label_opts=None,
      color='#51565C', symbol_size=8)

# RDC位置加入地图
RDC_cords = {
    gis[gis['城市代码'] == gys_rdc.iloc[i]['RDC_Name']].iloc[0, 4].split('市')[0] + '仓': [gys_rdc.iloc[i]['RDC_LGT'],
                                                                                     gys_rdc.iloc[i]['RDC_LAT']] for i
    in
    range(len(gys_rdc))}  ##会自动去重
for key, value in RDC_cords.items():
    c.add_coordinate(key, value[0], value[1])
c.add("RDC",
      [[rdc, 1] for rdc in list(RDC_cords.keys())],
      type_="scatter",
      label_opts=opts.LabelOpts(formatter="{b}", color='red', font_size=16, font_weight='bold'),
      color='#FF0000', symbol_size=8)

##CDC与RDC的连线
# 循环添加每一个覆盖
colors = {'572': "#f948f7", '24': "#d1c667", '28': "#c76813", '311': "#2c12ef", '760': "#231439", '531': "#65f2d6",
          '711': "#bdef0a"}  ##,"#a21c68","#e38105","#42d1bd"]
for i in list(gys_rdc['RDC_Name'].drop_duplicates()):
    data_rdc = gys_rdc[gys_rdc['RDC_Name'] == i]
    flow = []
    for j in range(0, len(data_rdc)):
        flow.append((list(data_rdc['CDC_Name'])[j], list(data_rdc['RDC_Name'])[j]))
    cang_name = gis[gis['城市代码'] == i].iloc[0, 4].split('市')[0] + '仓'
    c.add(series_name=cang_name, data_pair=flow, type_=GeoType.LINES,
          linestyle_opts=opts.LineStyleOpts(width=0.8, color=colors[i]),
          label_opts=opts.LabelOpts(is_show=False),
          ##trail_length=0,
          is_polyline=True,
          effect_opts=opts.EffectOpts(symbol='pin', symbol_size=10)
          )
c.set_global_opts(title_opts=opts.TitleOpts(title="供应商覆盖"))
c.render("供应商覆盖.html")








