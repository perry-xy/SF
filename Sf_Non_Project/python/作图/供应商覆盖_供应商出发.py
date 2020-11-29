##供应商覆盖连线—pyecharts==1.7.1
import pandas as pd
from pyecharts import options as opts
from pyecharts.charts import Geo
from pyecharts.globals import ChartType, SymbolType
from pyecharts.faker import Faker
from pyecharts.globals import GeoType

# 数据读入
path_gysrdc = u"D:/Code/Facility_Location_WL_Own/CDC_Network_7_22_769.csv"
path_gys = "d:/物料专配/3.0/0923物料数据处理/供应商.xlsx"
gys_rdc = pd.read_csv(open(path_gysrdc))
gys_total = pd.read_excel(path_gys, sheet_name='factory')
gis = pd.read_excel(path_gys, sheet_name='GIS')
sku_df = pd.read_excel('D:/Code/Sf_Non_Project/python/现状计算/current_status_input.xlsx', sheet_name='SKU_Info')

gys_rdc[['CDC_Name', 'RDC_Name']] = gys_rdc[['CDC_Name', 'RDC_Name']].astype(int).astype(str)
gys_total['城市代码'] = gys_total['城市代码'].astype(int).astype(str)
gis['城市代码'] = gis['城市代码'].astype(int).astype(str)

category_list=list()
sku=dict()
for idx, row in sku_df.iterrows():
    category_list.append(row['SKU代码'])
    sku[row['SKU代码']] = {'name': row['SKU名称'],
                                      'turnoverdays': row['周转天数'],
                                      'area_weight_ratio': row['面积重量比（m^2/kg)']}
category_list.sort()

GIS=dict()
for idx, row in gis.iterrows():
    GIS[row['城市代码']] = {'city_name_cn': row['城市名'],
                                     'city_name_en': row['city_name'],
                                     'lng': row['lgt'],
                                     'lat': row['lat'],}

# 取出未使用的供应商
gys_unuse_df = pd.merge(left=gys_total, right=gys_rdc, how='left', left_on='城市代码', right_on='CDC_Name')
gys_unuse = gys_unuse_df[list(gys_unuse_df['CDC_Name'].isna())].drop_duplicates()['城市代码']

# 初始化地图
c = Geo(init_opts=opts.InitOpts(width='1200px', height='700px'))
c.add_schema(
    maptype="china",
    itemstyle_opts=opts.ItemStyleOpts(color="#F0CA00", border_color="#111"))

# CDC位置加入地图坐标
CDC_cords = {GIS[gys_total.iloc[i]['城市代码']]['city_name_cn']: (gys_total.iloc[i]['CDC_LGT'], gys_total.iloc[i]['CDC_LAT']) for i in
             range(len(gys_total))}  ##会自动去重
for key, value in CDC_cords.items():
    c.add_coordinate(key, value[0], value[1])

# 未被选择的CDC加入地图坐标
unuse_cdc_sku = {'763':'包装胶袋','746':'包装胶袋','754':'包装胶袋','591':'防护用品,封箱胶纸,配饰用品,称重工具,贴纸类',\
                   '553':'称重工具','762':'封条类','532':'封条类','577':'封条类,一次性编织袋','592':'封箱胶纸',\
                   '535':'配饰用品','631':'夏装','756':'配饰用品,贴纸类,纸质运单'}
unuse_cdc_cords = {GIS[key]['city_name_cn'] + ':' + value : (GIS[key]['lng'], GIS[key]['lat']) \
                   for key,value in unuse_cdc_sku.items()}
for key, value in unuse_cdc_cords.items():
    c.add_coordinate(key, value[0], value[1])

# RDC位置加入地图坐标
RDC_cords = {GIS[gys_rdc.iloc[i]['RDC_Name']]['city_name_cn'].split('市')[0] + '仓': [gys_rdc.iloc[i]['RDC_LGT'],\
             gys_rdc.iloc[i]['RDC_LAT']] for i in range(len(gys_rdc))}  ##会自动去重
for key, value in RDC_cords.items():
    c.add_coordinate(key, value[0], value[1])

# 使用的CDC加入地图
c.add("被选择的供应商",
      [[GIS[use_cdc_city]['city_name_cn'], 1] for use_cdc_city in gys_rdc['CDC_Name'].drop_duplicates()],
      type_="scatter", label_opts=None,
      color='#3481B8', symbol_size=8)
# 未使用的CDC加入地图
c.add("未被选择的供应商",
      [[key, 1] for key in unuse_cdc_cords.keys()],
      type_="scatter", label_opts=None,
      color='#51565C', symbol_size=8)
c.add("RDC",
      [[rdc, 1] for rdc in list(RDC_cords.keys())],
      type_="scatter",
      label_opts=opts.LabelOpts(formatter="{b}", color='red', font_size=16, font_weight='bold'),
      color='#FF0000', symbol_size=8)

#RDC仓覆盖
m=0
for i in list(gys_rdc['RDC_Name'].drop_duplicates()):
    data_rdc = gys_rdc[gys_rdc['RDC_Name'] == i]
    flow = []
    for j in range(0, len(data_rdc)):
        CDC_Name = GIS[list(data_rdc['CDC_Name'])[j]]['city_name_cn']
        RDC_Name = GIS[list(data_rdc['RDC_Name'])[j]]['city_name_cn']
        flow.append((CDC_Name,RDC_Name))
    cang_name = gis[gis['城市代码'] == i].iloc[0, 4].split('市')[0] + '仓'
    c.add(series_name=cang_name, data_pair=flow, type_=GeoType.LINES,
          linestyle_opts=opts.LineStyleOpts(width=0.8, color='black'),
          label_opts=opts.LabelOpts(is_show=False),
          ##trail_length=0,
          is_polyline=True,
          effect_opts=opts.EffectOpts(symbol='pin', symbol_size=10),
          is_selected=False
          )
    m += 1

colors = ["#f948f7", "#d1c667", "#c76813", "#2c12ef",  "#231439", "#65f2d6", "#bdef0a","#a21c68","#e38105","#42d1bd",
          "#CB8D73", "#EC7C25", "#AF8A54", "#992572", "#0E4243", "#673831"]
##CDC与RDC的连线
# 循环添加每一个覆盖
for i in range(len(category_list)):
    sku_gys=gys_rdc.loc[gys_rdc[category_list[i]] > 0]
    flow=[]
    for j in range(0,len(sku_gys)):
        CDC_Name = GIS[list(sku_gys['CDC_Name'])[j]]['city_name_cn']
        RDC_Name = GIS[list(sku_gys['RDC_Name'])[j]]['city_name_cn'].split('市')[0] + '仓'
        flow.append((CDC_Name, RDC_Name))
    sku_name = sku[category_list[i]]['name']
    c.add(series_name=sku_name, data_pair=flow, type_=GeoType.LINES,
          linestyle_opts=opts.LineStyleOpts(width=0.8, color=colors[i]),
          label_opts=opts.LabelOpts(is_show=False),
          ##trail_length=0,
          is_polyline=True,
          effect_opts=opts.EffectOpts(symbol='pin', symbol_size=10),
          is_selected=False
          )

c.set_global_opts(title_opts=opts.TitleOpts(title="供应商覆盖", pos_top='50%'))
c.render("供应商覆盖关系_东莞_天津.html")