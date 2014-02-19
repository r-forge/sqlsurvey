 
print.sqlrepsurvey<-function(x,...){
  cat("MonetDB survey object with replicate weights:\n")
  print(x$call)
  invisible(x)
}

dim.sqlrepsurvey<-function(x){
  if(is.null(x$subset))
    nrows<-dbGetQuery(x$conn, sqlsubst("select count(*) from %%table%%", list(table=x$table)))[[1]]
  else
    nrows<-dbGetQuery(x$conn, sqlsubst("select count(*) from %%subtable%%",
                                       list(subtable=x$subset$table)))[[1]]
  ncols<-length(allVarNames(x))
  c(nrows,ncols)
}

dimnames.sqlrepsurvey<-function(x,...) list(character(0), allVarNames(x))

subset.sqlrepsurvey<-function(x,subset,...){

  subset<-substitute(subset)
  rval<-new.env()
  rval$subset<-sqlexpr(subset)

  rval$table<-basename(tempfile("_sbs_"))
  rval$idx<-basename(tempfile("_idx_"))

  allv<-all.vars(subset)

  tablename<-x$table

  if (any(sapply(allv, isCreatedVariable, design=x))){
    updates<-getTableWithUpdates(x,allv,c(x$weights,x$key),tablename)
    tablename<-updates$table
    alreadydefined<-dbGetQuery(x$conn, paste("select count(*) from functions where name = '",updates$fnames[1],"'",sep=""))[1,1]>0
    if(!alreadydefined){
      on.exit(for (d in updates$destroyfns) dbSendUpdate(x$conn,d), add=TRUE)
      for(f in updates$createfns) dbSendUpdate(x$conn,f)
    }
  } 
  
  
  rval$weights<-x$weights
  rval$repweights<-x$repweights
  if (is.null(x$subset)){
    query<-sqlsubst("create table %%tbl%% as (select %%key%% from %%base%% where %%subset%%) with data",
                    list(tbl=rval$table, key=x$key,subset=rval$subset, base=tablename )
                    )
  } else{
    query<-sqlsubst("create table %%tbl%% as (select %%key%% from %%oldsubset%% inner join %%base%% using(%%key%%) where %%subset%%) with data",
                    list(tbl=rval$table, key=x$key,subset=rval$subset, base=tablename, oldsubset=x$subset$table )
                )
    
  }
  dbSendUpdate(x$conn, query)
  dbSendUpdate(x$conn,sqlsubst("create unique index %%idx%% on %%tbl%%(%%key%%)",
                             list(idx=rval$idx,tbl=rval$table, key=x$key)))
  rval$conn<-x$conn
  reg.finalizer(rval, finalizeSubset)
  x$subset<-rval
  x$call<-sys.call(-1)
  x
}

sqlrepsurvey<-function(weights, repweights, scale,rscales,
                       driver=MonetDB.R(),database, table.name,
                       key="row_names",mse=FALSE,check.factors=10,degf=NULL,...){

    if(is(database,"DBIConnection")){
        db<-database
    } else{
        db<-dbConnect(driver,database,...)
    }
  
  if (is.data.frame(check.factors)){
    zdata<-check.factors
    actualnames<-dbListFields(db,table.name)
    if (!all(names(zdata) %in% actualnames)) stop("supplied data frame includes variables not in the data table")
    if (!all(actualnames %in% names(zdata))) message("levels for some variables not supplied: assumed numeric")
    for(v in setdiff(actualnames,names(zdata))) zdata[[v]]<-numeric(0)   
  } else{
    zdata<-make.zdata(db,table.name,factors=check.factors)
  }
  
  if (length(repweights)==1) {
    cat("repweights expression:",repweights,"expanded to:\n")
    repweights<-grep(repweights,dbListFields(db,table.name),value=TRUE)
    print(repweights)
  }

  if (is.null(degf)) degf<-length(repweights)-1
  
  rval<-list(conn=db, table=table.name, data=database,
           weights=weights,repweights=repweights,
           call=sys.call(), zdata=zdata, key=key,degf=degf
           )
  
  rval$mse<-mse
  rval$scale<-scale
  rval$rscales<-rscales
  
  class(rval)<-"sqlrepsurvey"
  rval
}




svytotal.sqlrepsurvey<-function(x, design, na.rm=TRUE, byvar=NULL, se=TRUE,...){

  design<-dropmissing(x,design,na.rm=na.rm)
  
  ## handle subpopulations
  if (is.null(design$subset)){
    tablename<-design$table
    wtname<-design$weights
    repweights<-design$repweights
  } else {
    tablename<-sqlsubst(" %%tbl%% inner join %%subset%% using(%%key%%) ",
                        list(tbl=design$table, subset=design$subset$table, key=design$key))
    wtname<-design$subset$weights
    repweights<-design$subset$repweights
  }

  if(!se) repweights<-NULL
  

  updates<-NULL
  metavars<-NULL
  allv<-unique(c(all.vars(x),all.vars(byvar)))
  needsUpdates<-any(!sapply(allv,isBaseVariable,design=design))
  if (needsUpdates){
    metavars<-with(design,c(wtname,repweights,key))
    updates<-getTableWithUpdates(design,allv,metavars,tablename)
    tablename<-updates$table
    on.exit(for (d in updates$destroyfns) dbSendUpdate(design$conn,d),add=TRUE)
    for(f in updates$createfns) dbSendUpdate(design$conn,f)
  }

  
  mf<- zero.model.frame(x,design)  
  if (!is.null(byvar)) byvar<-attr(terms(byvar),"term.labels")

  xcut<-cutprocess(x)
  if (length(xcut[[2]])) {
    for(fn in xcut[[2]]){
      dbSendUpdate(design$con, fn$construct)
      on.exit(dbSendUpdate(design$con, fn$destroy),add=TRUE)
    }
  }
  rvars<-attr(terms(x),"variables")[-1]
  tms<-terms(xcut[[1]])
  termterms<-sapply(attr(tms,"term.labels"),function(t) sqlexpr(parse(text=t)[[1]]))
  termnames<-make.db.names(design$con,termterms)
  tablename<-paste("(select",paste( c(wtname,repweights,byvar,paste(termterms,termnames,sep=" as ")),collapse=","),"from",tablename," ) as foo")
  
  results<-lapply(seq_along(termnames), function(i){
  	v<-termnames[i]
        e<-rvars[[i]]
    if (is.factor(eval(e,mf)) | is.character(eval(e,mf))){
      query<-paste("select ",paste(c(paste("sum(",c(wtname,repweights),")"),v,byvar),collapse=","),
                   "from",tablename,
                   "group by",paste(c(v,byvar),collapse=","),
                   "order by", paste(c(byvar,v),collapse=","))
      dbGetQuery(design$conn,query)
    } else {
      if(is.null(byvar)) {
        query<-paste("select ",paste(c(paste("sum((1*(",v,"))*",c(wtname,repweights),")"),adquote(v)),collapse=","),
                     "from",tablename)
      } else {
        query<-paste("select ",paste(c(paste("sum((1*(",v,"))*",c(wtname,repweights),")"),adquote(v),byvar),collapse=","),
                     "from",tablename,
                     "group by",paste(byvar,collapse=","),
                     "order by", paste(byvar,collapse=","))
      }
      dbGetQuery(design$conn,query)
    }
  })
  M<-length(repweights)
  totals<-do.call(rbind,lapply(results, function(r) as.matrix(r[,(1:(1+M))])))
  total<-totals[,1]
  names(total)<-do.call(c,lapply(results, function(r) apply(as.matrix(r[,-(1:(1+M)),drop=FALSE]),1,paste,collapse=":")))
  if (se) {
  	attr(total,"statistic")<-"total"
    attr(total,"var")<-svrVar(t(totals[,-1,drop=FALSE]),scale=design$scale,rscales=design$rscales,mse=design$mse, coef=total)
    class(total)<-"svrepstat"
  }

  total
}

svymean.sqlrepsurvey<-function(x, design, na.rm=TRUE, byvar=NULL, se=TRUE,...){
  design<-dropmissing(x,design,na.rm=na.rm)

  tms<-terms(x)

  ## handle subpopulations
  if (is.null(design$subset)){
    tablename<-design$table
    wtname<-design$weights
    repweights<-design$repweights
  } else {
    tablename<-sqlsubst(" %%tbl%% inner join %%subset%% using(%%key%%) ",
                        list(tbl=design$table, subset=design$subset$table, key=design$key))
    wtname<-design$subset$weights
    repweights<-design$subset$repweights
  }
  M<-length(repweights)

  updates<-NULL
  metavars<-NULL
  allv<-unique(c(all.vars(x),all.vars(byvar)))
  needsUpdates<-any(!sapply(allv,isBaseVariable,design=design))
  if (needsUpdates){
    metavars<-with(design,c(wtname,repweights,key))
    updates<-getTableWithUpdates(design,allv,metavars,tablename)
    tablename<-updates$table
    on.exit(for (d in updates$destroyfns) dbSendUpdate(design$conn,d),add=TRUE)
    for(f in updates$createfns) dbSendUpdate(design$conn,f)
  }

  
  mf<- zero.model.frame(x,design)

  
  if (!is.null(byvar)) byvar<-attr(terms(byvar),"term.labels")

  xcut<-cutprocess(x)
  if (length(xcut[[2]])) {
    for(fn in xcut[[2]]){
      dbSendUpdate(design$con, fn$construct)
      on.exit(dbSendUpdate(design$con, fn$destroy),add=TRUE)
    }
  }
  rvars<-attr(terms(x),"variables")[-1]
  tms<-terms(xcut[[1]])
  termterms<-sapply(attr(tms,"term.labels"),function(t) sqlexpr(parse(text=t)[[1]]))
  termnames<-make.db.names(design$con,termterms)
  tablename<-paste("(select",paste( c(wtname,repweights,byvar,paste(termterms,termnames,sep=" as ")),collapse=","),"from",tablename," ) as foo")
  
  if (is.null(byvar))
    qtotalwt<-paste("select",paste( paste("sum(",c(wtname,repweights),")"),collapse=","),"from",tablename)
  else
    qtotalwt<-paste("select",paste( paste("sum(",c(wtname,repweights),")"),collapse=","),"from",tablename, "group by", paste(byvar,collapse=","),"order by", paste(byvar,collapse=","))
    
  totalwt<-dbGetQuery(design$conn,qtotalwt)
  results<-lapply(seq_along(termnames), function(i){
    v<-termnames[i]
    e<-rvars[[i]]
    if (is.factor(eval(e,mf)) || is.character(eval(e,mf))){
      query<-paste("select ",paste(c(paste("sum(",c(wtname,repweights),")"),v,byvar),collapse=","),"from",tablename,"group by",paste(c(v,byvar),collapse=","),"order by", paste(c(byvar,v),collapse=","))
      total<-dbGetQuery(design$conn,query)
    } else {
      if(is.null(byvar)) {
        query<-paste("select ",paste(c(paste("sum((1*(",v,"))*",c(wtname,repweights),")"),adquote(v)),collapse=","),"from",tablename)
      } else {
        query<-paste("select ",paste(c(paste("sum((1*(",v,"))*",c(wtname,repweights),")"),adquote(v),byvar),collapse=","),"from",tablename,"group by",paste(byvar,collapse=","),"order by", paste(byvar,collapse=","))
      }
      total<-dbGetQuery(design$conn,query)
    }
    reps<-nrow(total)/nrow(totalwt)
    totalwts<-totalwt[rep(1:nrow(totalwt),each=reps),]
    total[,1:(M+1)]<-total[,1:(M+1)]/totalwts
    total
  })

  
  means<-do.call(rbind,lapply(results, function(r) as.matrix(r[,(1:(1+M))])))
  mean<-means[,1]
  names(mean)<-do.call(c,lapply(results, function(r) apply(as.matrix(r[,-(1:(1+M)),drop=FALSE]),1,paste,collapse=":")))
  if (se) {
      attr(mean,"statistic")<-"mean"
      attr(mean,"var")<-svrVar(t(means[,-1,drop=FALSE]),scale=design$scale,rscales=design$rscales,mse=design$mse, coef=mean)
  }
  class(mean)<-"svrepstat"
  mean
}  




svrVar<-function (thetas, scale, rscales, na.action = getOption("na.action"), 
    mse = getOption("survey.replicates.mse"), coef) 
{
    thetas <- get(na.action)(thetas)
    naa <- attr(thetas, "na.action")
    if (!is.null(naa)) {
        rscales <- rscales[-naa]
        if (length(rscales)) 
            warning(length(naa), " replicates gave NA results and were discarded.")
        else stop("All replicates contained NAs")
    }
    if (is.null(mse)) 
        mse <- FALSE
    if (length(dim(thetas)) == 2) {
        if (mse) {
            meantheta <- coef
        }
        else {
            meantheta <- colMeans(thetas[rscales > 0, , drop = FALSE])
        }
        v <- crossprod(sweep(thetas, 2, meantheta, "-") * sqrt(rscales)) * 
            scale
    }
    else {
        if (mse) {
            meantheta <- coef
        }
        else {
            meantheta <- mean(thetas[rscales > 0])
        }
        v <- sum((thetas - meantheta)^2 * rscales) * scale
    }
    attr(v, "na.replicates") <- naa
    attr(v, "means") <- meantheta
    return(v)
}


svylm.sqlrepsurvey<-function(formula, design,...){
    design<-dropmissing(formula,design,na.rm=TRUE)

  tms<-terms(formula)
  yname<-as.character(attr(tms,"variables")[[2]])
  ## handle subpopulations
  if (is.null(design$subset)){
    tablename<-design$table
    wtname<-design$weights
    repweights<-design$repweights
  } else {
    tablename<-sqlsubst(" %%tbl%% inner join %%subset%% using(%%key%%) ",
                        list(tbl=design$table, subset=design$subset$table, key=design$key))
    wtname<-design$subset$weights
    repweights<-design$subset$repweights
  }
  

  mm<-sqlmodelmatrix(formula, design, fullrank=TRUE)
  termnames<-mm$terms
  tablename<-sqlsubst("%%tbl%% inner join %%mm%% using(%%key%%)",
                        list(tbl=tablename,mm=mm$table, key=design$key))

   p<-length(termnames)
   mfy<-basename(tempfile("_y_"))
   sumxy<-paste("sum(",termnames,"*%%mfy%%*%%wt%%) as _xy_",termnames,sep="")
   sumsq<-outer(termnames,termnames, function(i,j) paste("sum(",i,"*",j,"*%%wt%%)",sep=""))

   qxwx<-sqlsubst("select %%sumsq%% from %%table%%" ,
                     list(sumsq=sumsq, table=tablename, wt=wtname)
                     )
   xwx<-matrix(as.matrix(dbGetQuery(design$conn, qxwx)),p,p)
   qxwy <- sqlsubst("select %%sumxy%% from %%tablename%% inner join (select %%y%% as %%mfy%%, %%key%% from %%mf%%) as _alias_ using(%%key%%)", 
        list(sumxy = sumxy, y = yname, key = design$key, tablename = tablename, 
        mf=mm$mf, wt=wtname, mfy=mfy))
   xwy<-drop(as.matrix(dbGetQuery(design$conn, qxwy)))
   beta<-qr.coef(qr(xwx),xwy)

  ##se
    replicates<-matrix(NA,nrow=length(design$repweights),ncol=p)
    for(i in seq_along(repweights)){
      qxwx<-sqlsubst("select %%sumsq%% from %%table%%" ,
                     list(sumsq=sumsq, table=tablename, wt=repweights[i])
                     )
      xwx<-matrix(as.matrix(dbGetQuery(design$conn, qxwx)),p,p)
      qxwy <- sqlsubst("select %%sumxy%% from %%tablename%%  inner join (select %%y%% as %%mfy%%, %%key%% from %%mf%%) as _alias_ using(%%key%%)", 
                       list(sumxy = sumxy, y = yname, key = design$key, tablename = tablename, 
                            mf=mm$mf, wt=repweights[i], mfy=mfy))
      xwy<-drop(as.matrix(dbGetQuery(design$conn, qxwy)))
      replicates[i,]<-qr.coef(qr(xwx),xwy)
    }
    singular<-is.na(beta)
    beta<-beta[!singular]
    if(all(singular)) stop("coefficient estimates all NA")
    if(any(singular)) warning("some coefficients not estimable; discarded")
    replicates<-replicates[,!singular]
    v<-svrVar(replicates,design$scale,design$rscales,na.action=getOption("na.action"), mse=design$mse, beta)
    
    names(beta)<-termnames[!singular]
    dimnames(v)<-list(termnames[!singular], termnames[!singular])
    class(beta)<-"svrepstat"
    attr(beta, "var")<-v
    attr(beta,"statistic")<-"coef"
    beta
  }


close.sqlrepsurvey<-function(con, ...){
  gc() ## try to make sure any dead model matrices are finalized.
  dbDisconnect(con$conn)
}

open.sqlrepsurvey<-function(con, driver, ...){  
  con$conn<-dbConnect(driver, url=con$data,...)
  if (!is.null(con$subset)){
    con$subset$conn<-con$conn
  }
  con
}


findQuantile<-function(xname,wtname,repweights, tablename, design, quantile,...){
   N<-dim(design)[1]
   n<-round(sqrt(N))*10


   samp <- dbGetQuery(design$conn, paste("select ",xname," as x_,",wtname, "as wt_,",paste(repweights,collapse=",")," from ",tablename, "sample",n))
   tempdes<-svrepdesign(data=samp, weights=~wt_,  repweights=samp[,-(1:2)],scale=design$scale,rscales=design$rscales,mse=design$mse,type="other")
   guess<-svyquantile(~x_, design=tempdes, quantiles=quantile,se=TRUE,na.rm=TRUE)

   if (SE(guess)==0) return(as.numeric(guess))
   
   lower<-guess-4*SE(guess)
   upper<-guess+4*SE(guess)

   totwt<-dbGetQuery(design$conn, sqlsubst("select sum(%%wtname%%) from %%table%% where %%x%% is not null", list(wtname=wtname,table=tablename,x=xname)))[[1]]
   
   tries<-0
   maxtries<-5
   while(tries < maxtries){
     ltail<-dbGetQuery(design$conn,
                       sqlsubst("select sum(%%wtname%%) from %%table%% where (%%x%%<%%lower%%)",
                                list(wtname=wtname,table=tablename,x=xname,lower=lower)))[[1]]
     utail<-dbGetQuery(design$conn,
                       sqlsubst("select sum(%%wtname%%) from %%table%% where (%%x%%>%%upper%%)",
                                list(wtname=wtname,table=tablename,x=xname,upper=upper)))[[1]]
     
     if (!is.na(ltail) && ltail >= quantile*totwt) {
       upper<-lower
       lower<-lower-4*SE(guess)
     } else if (!is.na(utail) && utail >= (1-quantile)*totwt){
       lower<-upper
       upper<-upper+4*SE(guess)
     } else break
     
     tries<-tries+1
     cat("missed quantile, retrying\n")
   }
   if (tries>=maxtries) stop("quantile failed")
   
   samp<-dbGetQuery(design$conn,
                    sqlsubst("select %%x%% as x_, %%wtname%% as wt_ from %%table%% where ((%%x%%>=%%lower%%) and (%%x%%<=%%upper%%))",
                             list(x=xname, wtname=wtname, lower=lower,upper=upper,table=tablename)))
   samp<-samp[order(samp$x_),]
   samp$cumwt<-cumsum(samp$wt_)
   
   offset<-as.numeric((quantile*totwt)-ltail)
   samp$x_[min(which(samp$cumwt>=offset))]
   
}

svyquantile.sqlrepsurvey<-function(x,design, quantiles,se=FALSE,na.rm=TRUE,...){
  design<-dropmissing(x,design,na.rm=na.rm)

  if (is.null(design$subset)){
    tablename<-design$table
    wtname<-design$weights
    repweights<-design$repweights
  } else {
    tablename<-sqlsubst("%%tbl%% inner join %%sub%% using(%%key%%)",
                        list(tbl=design$table, sub=design$subset$table,
                             key=design$key))
    wtname<-design$subset$weights
    repweights<-design$subset$repweights
  }

  
  updates<-NULL
  metavars<-NULL
  allv<-all.vars(x)
  needsUpdates<-any(!sapply(allv,isBaseVariable,design=design))
  if (needsUpdates){
    metavars<-with(design,c(wtname,repweights,key))
    updates<-getTableWithUpdates(design,allv,metavars,tablename)
    tablename<-updates$table
    on.exit(for (d in updates$destroyfns) dbSendUpdate(design$conn,d),add=TRUE)
    for(f in updates$createfns) dbSendUpdate(design$conn,f)
  }

  mf<- zero.model.frame(x,design)
  
  if(length(quantiles)>1) stop("only one quantile")
  tms<-terms(x)
  rval<-sapply(attr(tms,"term.labels"),
               function(v) if (is.numeric(mf[[v]])) findQuantile(v,wtname,repweights,tablename,design,quantiles) else NaN)
  names(rval)<-attr(tms,"term.labels")
  
  if(se){
    ci<-sapply(attr(tms,"term.labels"), function(varname){
      totwt<-dbGetQuery(design$conn, paste("select sum(",wtname,") from",tablename," where ",varname,"is not null"))
      replicates<-dbGetQuery(design$conn,
                             paste("select ",paste(paste("sum(",c(wtname,repweights),")"),collapse=","),"from",tablename,"where (",varname,">",quantiles,")"))
      var<-svrVar(t(as.matrix(replicates)[,-1,drop=FALSE]/as.vector(as.matrix(totwt))), scale=design$scale, rscales=design$rscales,
                  mse=design$mse, coef=as.matrix(replicates)[,1]/as.vector(as.matrix(totwt)))
      upper<-findQuantile(varname, wtname,repweights,tablename,design,quantiles+1.96*sqrt(var))
      lower<-findQuantile(varname, wtname,repweights,tablename,design,quantiles-1.96*sqrt(var))
      c(lower=lower,upper=upper,se=(upper-lower)/(2*1.96))
    })
    attr(rval,"ci")<-ci
    rval
  }
  
  rval
}


svyloglin.sqlrepsurvey<-function (formula, design, ...) 
{
    if (length(formula) != 2) 
        stop("needs a one-sided formula")
  design<-dropmissing(formula,design,na.rm=TRUE)

    if (is.null(design$subset)) {
        tablename <- design$table
        wtname <- design$weights
        repweights <- design$repweights
    }
    else {
        tablename <- sqlsubst(" %%tbl%% inner join %%subset%% using(%%key%%) ", 
            list(tbl = design$table, subset = design$subset$table, 
                key = design$key))
        wtname <- design$subset$weights
        repweights <- design$subset$repweights
    }
    
    n <- nrow(design)
    vars<-all.vars(formula)
    p<-length(vars)
    

    updates<-NULL
    metavars<-NULL
    allv<-all.vars(formula)
    needsUpdates<-any(!sapply(allv,isBaseVariable,design=design))
    if (needsUpdates){
      metavars<-with(design,c(wtname,repweights,key))
      updates<-getTableWithUpdates(design,allv,metavars,tablename)
      tablename<-updates$table
      on.exit(for (d in updates$destroyfns) dbSendUpdate(design$conn,d),add=TRUE)
      for(f in updates$createfns) dbSendUpdate(design$conn,f)
    }

 
    qweights<-paste( paste("sum(",c(wtname,repweights),")"),collapse=",")
    qvars<-paste(vars,collapse=",")
	  totals<-dbGetQuery(design$conn, paste("select",qvars,",",qweights,"from",tablename,"group by",qvars,"order by", qvars))
	  allhatp<-sweep(as.matrix(totals[,-(1:p)]),2,colSums(totals[,-(1:p)]),"/")
	  hatp<-allhatp[,1]
	  V<-svrVar(t(allhatp[,-1]),scale=design$scale,rscales=design$rscales,mse=design$mse, coef=hatp)

    dat <- as.data.frame(lapply(totals[,1:p], as.factor))
    dat$y <- hatp * n
    ff <- update(formula, y ~ .)
    m1 <- withOptions(list(contrasts = c("contr.sum", "contr.poly")), 
        glm(ff, data = dat, family = quasipoisson))
    P1 <- (diag(fitted(m1)/n) - tcrossprod(fitted(m1)/n))/n

    XX <- model.matrix(m1)[, -1, drop = FALSE]
    XX <- sweep(XX, 2, colMeans(XX))
    Vtheta <- solve(t(XX) %*% P1 %*% XX) %*% (t(XX) %*% V %*% 
        XX) %*% solve(t(XX) %*% P1 %*% XX)/(n * n)
        
    class(hatp)<-"svrepstat"    
    attr(hatp,"var")<-V    
    rval <- list(model = m1, var = Vtheta, prob.table = hatp, 
        df.null = degf(design), n = n)
    call <- sys.call()
    ##call[[1]] <- as.name(.Generic)
    rval$call <- call
    class(rval) <- "svyloglin"
    rval
}
withOptions<-survey:::withOptions
degf.sqlrepsurvey<-function(design,...){
    if (!is.null(design$degf))
        design$degf
    else 
        length(design$repweights)
}

svychisq.sqlrepsurvey<-function(formula,design,pval = c("F", "saddlepoint", "lincom", "chisq"),...){
  ll0<-svyloglin(formula,design)
  ll1<-update(ll0,~.^2)
  a<-anova(ll0,ll1)
  pval <- match.arg(pval)

  rval<-list(call=sys.call(), X2=a$score$chisq, p=switch(pval, lincom = a$score$p[1], 
        saddlepoint = a$score$p[4], chisq = a$score$p[2], F = a$score$p[3]))
  rval
}



svytable.sqlrepsurvey<-function(formula, design,...){
  tms<-terms(formula)
  if (is.null(design$subset))
    tablename<-design$table
  else
    tablename<-sqlsubst("%%base%% inner join %%sub%% using(%%key%%) where %%wt%%>0",
                        list(base=design$table, sub=design$subset$table,
                             key=design$key, wt=design$subset$weights))


  updates<-NULL
  metavars<-NULL
  allv<-all.vars(formula)
  needsUpdates<-any(!sapply(allv,isBaseVariable,design=design))
  if (needsUpdates){
    metavars<-with(design,c(design$weights,repweights,key))
    updates<-getTableWithUpdates(design,allv,metavars,tablename)
    tablename<-updates$table
    on.exit(for (d in updates$destroyfns) dbSendUpdate(design$conn,d),add=TRUE)
    for(f in updates$createfns) dbSendUpdate(design$conn,f)
  }

  

  
  query<-sqlsubst("select sum(%%wt%%),%%tms%%  from %%table%% group by %%tms%% order by %%tms%%",
                  list(wt=design$weights,
                       table=tablename,
                       tms=attr(tms,"term.labels")))
  
  d<-dbGetQuery(design$conn, query)
  names(d)[1]<-"_count_"
   xtabs(`_count_`~.,data=d)
  
}
