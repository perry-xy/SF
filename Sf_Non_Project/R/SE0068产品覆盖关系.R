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
data_fugai=read.xlsx("d:/user/01397192/桌面/SE0068_fugai.xlsx",header=TRUE,sheetIndex = 1)

##所有需求点
xqd_org=leaflet(data=data_fugai)
xqd_map=amap(xqd_org)
xqd_map=addCircles(xqd_map,data=data_fugai,lng=~dest_lng,lat=~dest_lat,color="red",weight=1)

##所有RDC仓及RDC仓覆盖
rdc=c(22,27,28,572,769)
data_rdc=data_fugai[data_fugai$src_city %in% rdc,]
url="https://ss1.bdstatic.com/70cFuXSh_Q1YnxGkpoWK1HF6hhy/it/u=3930451151,1593616828&fm=26&gp=0.jpg"
redIcon <- makeIcon(
  iconUrl = url,
  iconWidth = 32, iconHeight = 32,
  iconAnchorX = 16, iconAnchorY = 16
)
xqd_map=addMarkers(xqd_map,data=data_rdc,lng=~src_lng,lat=~src_lat,icon=redIcon)
colors_=c("red","green","#FA842B","yellow","purple")
for (i in c(1:length(rdc))){
  data=data_rdc[data_rdc$src_city==rdc[i],]
  for (j in c(1:nrow(data))){
    xqd_map=addPolylines(xqd_map,lng=c(data$src_lng[j],data$dest_lng[j]),lat=c(data$src_lat[j],data$dest_lat[j]),color=colors_[i],weight=2)
  }
}

##所有区仓及其覆盖
dc=c(573,21,29,10,851,513,571,20,25,755,535,871,7311,512,931,574,
23,451,577,517,371,316,579,752,757)
data_dc=data_fugai[data_fugai$src_city %in% dc,]
dc_url='https://ss1.bdstatic.com/70cFvXSh_Q1YnxGkpoWK1HF6hhy/it/u=2555040943,474183672&fm=26&gp=0.jpg'
blueIcon <- makeIcon(
  iconUrl = dc_url,
  iconWidth = 22, iconHeight = 22,
  iconAnchorX = 11, iconAnchorY = 11
)
xqd_map=addMarkers(xqd_map,data=data_dc,lng=~src_lng,lat=~src_lat,icon=blueIcon)
##xqd_map
##colors_=c("red","green","yellow","orange","purple")
for (i in c(1:length(dc))){
  data=data_dc[data_dc$src_city==dc[i],]
  for (j in c(1:nrow(data))){
    xqd_map=addPolylines(xqd_map,lng=c(data$src_lng[j],data$dest_lng[j]),lat=c(data$src_lat[j],data$dest_lat[j]),color='black',weight=2)
  }
}
xqd_map
