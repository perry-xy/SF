library(xlsx)
#线性回归
data=read.xlsx('d:/user/01397192/Downloads/顺陆成本1.5吨.xlsx',header=TRUE,sheetName='1.5吨')
data=read.table("clipboard",header=TRUE)
fit<-lm(price~distance,data=data)  
summary(fit)
hist(fit$residuals)
boxplot(fit$residuals)
qqnorm(fit$residuals);qqline(fit$residuals)

data=read.table("clipboard",header=TRUE)
data_1=data/10000
boxplot(data_1)
summary(data_1)
summary(data)