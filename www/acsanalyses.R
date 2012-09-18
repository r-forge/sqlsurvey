library(sqlsurvey)


## replace the path with wherever you put the .jar files
monetdriver<-MonetDB(classPath="/usr/local/monetdb/share/monetdb/lib/monetdb-jdbc-2.4.jar")

## this takes a long time, but only needs to be done once
acs<-sqlrepsurvey(weight="pwgtp", repweights=paste("pwgtp",1:80,sep=""), scale=4/80, rscales=rep(1,80), mse=TRUE, database="jdbc:monetdb://localhost/demo",
                  driver=monetdriver, key="idkey", table.name="acs3yr",check.factors=TRUE, user="monetdb",password="monetdb")

## do stuff

svytotal(~sex, acs, byvar=~st)
svymean(~wagp,acs, byvar=~sex)
svyquantile(~agep, acs,quantiles=0.5)

svyplot(wagp~wkhp,acs, style="hex")
par(mfrow=c(2,2))
svyplot(wagp~wkhp,acs, style="hex",col=ifelse(sex==1, "blue","magenta"))


##
acs<-close(acs)
save(acs, file="saved-design.rda")


### next time 
library(sqlsurvey)
## replace the path with wherever you put the .jar files
monetdriver<-MonetDB(classPath="/usr/local/monetdb/share/monetdb/lib/monetdb-jdbc-2.4.jar")
load("saved-design.rda")
acs<-open(acs, driver=monetdriver, user="monetdb",password="monetdb")

svychisq(~sex+st,acs)
plot(svysmooth(wagp~agep,design=acs,sample.bandwidth=5000))
## do more stuff


