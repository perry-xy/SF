##加载需要的包
options(java.parameters="-Xmx6144m")
options(java.parameters="-Xms6144m")
library(leaflet)
library(leafletCN)
library(xlsx)
library(dplyr)
library(rmarkdown)
library(tinytex)
Sys.setlocale(category = "LC_ALL", locale = "Chinese")

##读入覆盖关系
data_fugai=read.csv("d:/user/01397192/桌面/lxm/22_769/C_Network_7.csv",header=TRUE)

##所有需求点
xqd_org=leaflet(data=data_fugai)
xqd_map=amap(xqd_org)
xqd_map=addCircles(xqd_map,data=data_fugai,lng=~CUSTOMER_LGT,lat=~CUSTOMER_LAT,color="red",weight=1)

##所有RDC仓
data_RDC=unique(data_fugai[,c('RDC','RDC_LAT','RDC_LGT')])
#rdc=c(24,28,311,531,572,711,760)
#rdc=c(24,28,22,531,572,711,760)
rdc=c(24,28,22,531,572,711,769)
#rdc=c(24,28,22,0,572,27,769)
# url="https://ss1.bdstatic.com/70cFuXSh_Q1YnxGkpoWK1HF6hhy/it/u=3930451151,1593616828&fm=26&gp=0.jpg"
# redIcon <- makeIcon(
#   iconUrl = url,
#   iconWidth = 26, iconHeight = 26,
#   iconAnchorX = 13, iconAnchorY = 13
# )
#xqd_map=addMarkers(xqd_map,data=data_RDC,lng=~RDC_LGT,lat=~RDC_LAT,icon=redIcon)
xqd_map=addAwesomeMarkers(xqd_map,data=data_RDC,lng=~RDC_LGT,lat=~RDC_LAT,icon =makeAwesomeIcon(icon = "home",markerColor = "red"))
#xqd_map=addAwesomeMarkers(xqd_map,lng=114.502461,lat=38.045474,icon =makeAwesomeIcon(icon = "home",markerColor = "gray"))
#xqd_map=addAwesomeMarkers(xqd_map,lng=113.382391,lat=22.521113,icon =makeAwesomeIcon(icon = "home",markerColor = "gray"))
##所有覆盖
colors_=c("red","green","yellow","blue","grey","purple","black","brown","pink","orange","grey","purple","black","brown","pink","orange","#992572")
for (i in c(1:length(rdc))){
  data=data_fugai[data_fugai$RDC==rdc[i],]
  for (j in c(1:nrow(data))){
    xqd_map=addPolylines(xqd_map,lng=c(data$RDC_LGT[j],data$CUSTOMER_LGT[j]),lat=c(data$RDC_LAT[j],data$CUSTOMER_LAT[j]),color=colors_[i],weight=3)
  }
}
xqd_map


##供应商覆盖关系
##读入供应商与RDC仓覆盖关系及所有供应商
gys_rdc=read.csv("D:/物料专配/3.0/0923物料数据处理/0925结果/CDC_RDC_Network_7.csv",header = TRUE)
gys_total=read.xlsx("D:/物料专配/3.0/0923物料数据处理/供应商.xlsx",header=TRUE,sheetName ="factory",encoding='UTF-8')

##取出所有使用与未使用供应商与RDC仓
rdc=unique(gys_rdc[,c("RDC_Name","RDC_LAT","RDC_LGT")])
gys_total=gys_total[,c("城市代码","CDC_LGT","CDC_LAT")]
gys_use=unique(gys_rdc$CDC_Name)
gys_unuse=setdiff(gys_total$城市代码,gys_use)

##初始化地图
gys_org=leaflet()
gys_map=amap(gys_org)
##画供应商与RDC仓
gys_url="https://ss2.bdstatic.com/70cFvnSh_Q1YnxGkpoWK1HF6hhy/it/u=1146573554,1089138354&fm=15&gp=0.jpg"
RDC_url="https://timgsa.baidu.com/timg?image&quality=80&size=b9999_10000&sec=1599557354988&di=718453d30ba0d79834c4dd26408d6840&imgtype=0&src=http%3A%2F%2Fbpic.588ku.com%2Felement_pic%2F18%2F03%2F20%2F9198e6f96df9ad22dc05759b6c62c97a.jpg"
gysunuse_url='https://ss1.bdstatic.com/70cFvXSh_Q1YnxGkpoWK1HF6hhy/it/u=251172279,997863868&fm=26&gp=0.jpg'
##供应商覆盖关系
# gys_icon=makeIcon(iconUrl = gys_url,iconWidth = 32, iconHeight = 32,iconAnchorX = 19, iconAnchorY = 19)
# RDC_icon=makeIcon(iconUrl = RDC_url,iconWidth = 40, iconHeight = 40,iconAnchorX = 19, iconAnchorY = 19)
# gysunuse_icon=makeIcon(iconUrl = gysunuse_url,iconWidth = 32, iconHeight = 32,iconAnchorX = 19, iconAnchorY = 19)

gys_map=addAwesomeMarkers(gys_map,data=gys_total[gys_total$城市代码 %in% gys_use,],lng=~CDC_LGT,lat=~CDC_LAT,icon =makeAwesomeIcon(icon = "plus-sign",markerColor = "blue"))
#gys_map=addAwesomeMarkers(gys_map,data=rdc,lng=~RDC_LGT,lat=~RDC_LAT,icon =makeAwesomeIcon(icon = "home",markerColor = "red"))
#gys_map=addAwesomeMarkers(gys_map,data=gys_total[gys_total$城市代码 %in% gys_unuse,],lng=~CDC_LGT,lat=~CDC_LAT,icon =makeAwesomeIcon(icon = "plus-sign",markerColor = "gray"))
#gys_map
# gys_map=addMarkers(gys_map,data=rdc,lng=~RDC_LGT,lat=~RDC_LAT,icon=RDC_icon)
# gys_map=addMarkers(gys_map,data=gys_total[gys_total$城市代码 %in% gys_use,],lng=~CDC_LGT,lat=~CDC_LAT,icon=gys_icon)
# gys_map=addMarkers(gys_map,data=gys_total[gys_total$城市代码 %in% gys_unuse,],lng=~CDC_LGT,lat=~CDC_LAT,icon=gysunuse_icon)

##供应商与RDC仓库连线
##color_sku=c("red","green","yellow","blue","grey","purple","black","brown","pink","orange","grey","purple","black","brown","pink","orange")
color_sku=c("#F70000","#FA842B","#F5FF00","#8A5A83","#48A43F","#154889","#8F4E35","#0A0A0D","#7E8B92","#D47479",
            "#49392D","#07737A","#A65E2F","#939176","#D9C022","#992572")
sku=c('SKU1','SKU2','SKU3','SKU4','SKU5','SKU6','SKU7','SKU8','SKU9','SKU10','SKU11','SKU12','SKU13','SKU14','SKU15','SKU16')
for (i in c(1:length(sku))){
  data=gys_rdc[gys_rdc[,sku[i]]>0,]
  for (j in c(1:nrow(data))){
    gys_map=addPolylines(gys_map,lng=c(data$CDC_LGT[j],data$RDC_LGT[j]),lat=c(data$CDC_LAT[j],data$RDC_LAT[j]),color=color_sku[i],weight=3)
  }
  
}
gys_map

