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
data_fugai=read.xlsx("d:/user/01397192/桌面/dc_发货.xlsx",header=TRUE,sheetIndex=1,encoding='UTF-8')

##所有需求点
#data_fugai=data_fugai[data_fugai$TYPE=='Normal',]
xqd_org=leaflet(data=data_fugai)
xqd_map=amap(xqd_org)
xqd_map=addCircles(xqd_map,data=data_fugai,lng=~de_lng,lat=~de_lat,color="red",weight=1)

##所有RDC仓
data_RDC=unique(data_fugai[,c('warehouse_city','ware_lng','ware_lat')])
#rdc=c(24,28,311,531,572,711,760)
#rdc=c(24,28,22,531,572,711,760)
#rdc=c(24,28,22,531,572,711,769)
#rdc=c(24,28,22,0,572,27,769)
# url="https://ss1.bdstatic.com/70cFuXSh_Q1YnxGkpoWK1HF6hhy/it/u=3930451151,1593616828&fm=26&gp=0.jpg"
# redIcon <- makeIcon(
#   iconUrl = url,
#   iconWidth = 26, iconHeight = 26,
#   iconAnchorX = 13, iconAnchorY = 13
# )
#xqd_map=addMarkers(xqd_map,data=data_RDC,lng=~RDC_LGT,lat=~RDC_LAT,icon=redIcon)
xqd_map=addAwesomeMarkers(xqd_map,data=data_RDC,lng=~ware_lng,lat=~ware_lat,icon =makeAwesomeIcon(icon = "home",markerColor = "red"))
#xqd_map=addAwesomeMarkers(xqd_map,lng=114.502461,lat=38.045474,icon =makeAwesomeIcon(icon = "home",markerColor = "gray"))
#xqd_map=addAwesomeMarkers(xqd_map,lng=113.382391,lat=22.521113,icon =makeAwesomeIcon(icon = "home",markerColor = "gray"))
##所有覆盖
colors_=c("black","green","grey","yellow","red","purple","black","green","pink","orange","grey","purple","black","brown","pink","orange","#992572")
for (i in c(1:length(data_RDC$warehouse_city))){
  data=data_fugai[data_fugai$warehouse_city==data_RDC$warehouse_city[i],]
  for (j in c(1:nrow(data))){
    xqd_map=addPolylines(xqd_map,lng=c(data$ware_lng[j],data$de_lng[j]),lat=c(data$ware_lat[j],data$de_lat[j]),color='green',weight=3)
  }
}
xqd_map