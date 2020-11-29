##pyecharts==0.5.11版本的代码
from pyecharts import Map,Geo,Bar
import pandas as pd
from pyecharts import Funnel
import random
from pyecharts import GeoLines, Style


##世界地图
value = [95.1, 23.2, 43.3, 66.4, 88.5]
attr= ["China", "Canada", "Brazil", "Russia", "United States"]
map0=Map("世界地图示例",width=1200,height=600)
map0.add("世界地图",attr,value,maptype="world",is_visualmap=True, visual_text_color='#000')
map0.render("世界地图.html")

##中国地图
province_distribution = {'河南': 45.23, '北京': 37.56, '河北': 21, '辽宁': 12, '江西': 6, '上海': 20, '安徽': 10, '江苏': 16, '湖南': 9, '浙江': 13, '海南': 2, '广东': 22, '湖北': 8, '黑龙江': 11, '澳门': 1, '陕西': 11, '四川': 7, '内蒙古': 3, '重庆': 3, '云南': 6, '贵州': 2, '吉林': 3, '山西': 12, '山东': 11, '福建': 4, '青海': 1, '舵主科技，质量保证': 1, '天津': 1, '其他': 1}
province=list(province_distribution.keys())
values=list(province_distribution.values())
# maptype='china' 只显示全国直辖市和省级
# 数据只能是省名和直辖市的名称
map1=Map("中国地图示例",width=2000,height=900)
map1.add("中国地图",province,values,visual_range=[0,50],maptype="china",is_visualmap=True,
    visual_text_color='#000',is_label_show=True)
map1.render("中国地图.html")

##物料专配省份热力图
RDC_Province=pd.read_excel(u"D:/物料专配/1.0/中间文件/RDC配送流向流量.xlsx",sheet_name="Sheet8",usecols=["目的省份","求和项:2020年预估重量(KG）"])
RDC_Province.drop(index=[31],inplace=True)
RDC_Province.columns=["province","weight"]
province=list(RDC_Province['province'])
weight=list(RDC_Province['weight'])
# maptype='china' 只显示全国直辖市和省级
# 数据只能是省名和直辖市的名称
map2 = Map("物料专配需求分布", width=2000, height=900)
piece=[{'max':500000},
      {'min':600000,'max':1500000},
      {'min':1600000,'max':2600000},
      {'min':3000000,'max':6000000},
      {'min':8000000,'max':16000000},
      {'min':30000000}]
map2.add("省份需求分布", province, weight, visual_range=[0,40000000], maptype='china', is_visualmap=True,
    visual_text_color='#000',is_label_show=True,is_piecewise=True,pieces=piece)
map2.render("物料专配-省份分布.html")

##漏斗图
data_zhl=pd.read_excel(u"D:/code/Python/file/转化率作图.xlsx")
attr = data_zhl.环节
values = data_zhl.总体转化率
funnel0 = Funnel('总体转化漏斗图', title_pos='center',width=2000,height=900)
funnel0.add(name='环节',  # 指定图例名称
            attr=attr,  # 指定属性名称
            value=values,  # 指定属性所对应的值
            is_label_show=True,  # 确认显示标签
            label_formatter='{c}'+'%',  # 指定标签显示的方式
            legend_top='bottom',    # 指定图例位置，为避免遮盖选择右下展示
            # pyecharts包的文档中指出，当label_formatter='{d}'时,标签以百分比的形式显示.
            # 但我这样做的时候,发现显示的百分比与原始数据对应不上,只好用上面那种显示形式

            label_pos='outside',  # 指定标签的位置,inside,outside
            legend_orient='vertical',  # 指定图例显示的方向
            legend_pos='right')  # 指定图例的位置

funnel0.render("转化漏斗.html")

##pyecharts仓网覆盖连线
path=u"D:/物料专配/2.0/8.25版/4、7、10仓结果/C_Network_10_0826.csv"
data_pyfugai=pd.read_csv(open(path))
#获取RDC仓位置和城市经纬度坐标点
RDC_cords={data_pyfugai.iloc[i]['RDC_NAME']:[data_pyfugai.iloc[i]['RDC_LGT'],data_pyfugai.iloc[i]['RDC_LAT']] for i in range(len(data_pyfugai))}
City_cords = {data_pyfugai.iloc[i]['CUSTOMER_NAME']:[data_pyfugai.iloc[i]['CUSTOMER_LGT'],data_pyfugai.iloc[i]['CUSTOMER_LAT']] for i in range(len(data_pyfugai))}
#所有的覆盖流向
data_flow=data_pyfugai[["RDC_NAME","CUSTOMER_NAME"]]
#所有的RDC仓名放入列表
RDC_city=list(RDC_cords.keys())
#设置画布的格式
style = Style(title_pos="center",
       width=2000,
       height=900)
geolines = GeoLines('仓网覆盖图', **style.init_style)
##每一次add的名称
rdc_name=[]
for i in range(0,len(RDC_city)):
    x=RDC_city[i].split("市")
    y=x[0]+"仓"
    rdc_name.append(y)
#循环添加每一个覆盖
colors=[["#f948f7","#f948f7"],["#d1c667","#d1c667"],["#c76813","#c76813"],["#2c12ef","#2c12ef"],
["#231439","#231439"],["#65f2d6","#65f2d6"],["#bdef0a","#bdef0a"],["#a21c68","#a21c68"],["#e38105","#e38105"],["#42d1bd","#42d1bd"]]
for i in range(0,len(RDC_city)):
    data_rdc=data_flow[data_flow['RDC_NAME']==RDC_city[i]]
    flow=[]
    for j in range(0,len(data_rdc)):
        flow.append((data_rdc.iloc[j,0],data_rdc.iloc[j,1]))
    #部分地理轨迹图的格式
    style_geolines = style.add(is_label_show=False,
                                  line_curve=0.1,       #轨迹线的弯曲度，0-1
                                  line_opacity=0.6,      #轨迹线的透明度，0-1
                                  geo_effect_symbol='plane', #特效的图形，有circle,plane,pin等等
                                  geo_effect_symbolsize=8,  #特效图形的大小
                                  geo_effect_color='#7FFFD4', #特效的颜色
                                  geo_effect_traillength=0.1, #特效图形的拖尾效果，0-1
                                  label_color=colors[i],#轨迹线的颜色，标签点的颜色，
                                  border_color='#97FFFF',   #边界的颜色
                                  geo_normal_color='#36648B', #地图的颜色
                                  legend_pos = 'left',
                                  label_formatter='{b}')
    geolines.add(name=rdc_name[i],data=flow,maptype='china',  #地图的类型，可以是省的地方，如'广东',也可以是地市，如'东莞'等等
                     geo_cities_coords=City_cords,**style_geolines)
#发布，得到图形的html文件
geolines.render("py十仓仓网覆盖.html")

##堆叠柱状图
x = ["衬衫", "羊毛衫", "雪纺衫","裤子", "高跟鞋", "袜子"]
y1 =[5, 20, 36, 10, 75, 90]
y2 = [10, 25, 8, 60, 20, 80]
bar = Bar("柱状图数据堆叠示例")
title1 = '商家A'
title2 = '商家B'
bar.add(title1,x,y1,is_stack=True)
bar.add(title2,x,y2,is_stack=True)
bar.render('柱状图数据堆叠.html')


