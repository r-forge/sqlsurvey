## Setup for some blood pressure data from NHANES

load("nhanesbp.rda")
library(RMonetDB)
monetdriver<-MonetDB(classPath="/usr/local/monetdb/share/monetdb/lib/monetdb-jdbc-2.4.jar")
monet<-dbConnect(monetdriver,"jdbc:monetdb://localhost/demo",user="monetdb",password="monetdb")
nhanes<-subset(nhanes,!is.na(fouryearwt))
dbWriteTable(monet,"nhanesbp",nhanes)
dbDisconnect(monet)

## potentially in another session
monetdriver<-MonetDB(classPath="/usr/local/monetdb/share/monetdb/lib/monetdb-jdbc-2.4.jar")
sqhanes<-sqlsurvey(id="sdmvpsu",strata="sdmvstra",weights="fouryearwt",table.name="nhanesbp",database="jdbc:monetdb://localhost/demo",driver=monetdriver,user="monetdb",password="monetdb",key="seqn")

sqhanes
dim(sqhanes)
colnames(sqhanes)

sex<-svytotal(~riagendr,sqhanes,se=TRUE)
sex
attr(sex,"var")

bp<-svymean(~bpxsar+bpxdar,sqhanes,se=TRUE)
bp
attr(bp,"var")

svyplot(bpxsar~bpxdar,design=sqhanes,style="hex")
par(mfrow=c(2,2))
svyplot(bpxsar~bpxdar,design=sqhanes,style="subsample",col=ifelse(riagendr==1,"blue","magenta"))


svylm(bpxsar~bpxdar+riagendr,design=sqhanes)
