

make.zdata<-function(db, table, factors=9){
  rval<-dbGetQuery(db,sqlsubst("select * from %%tbl%% limit 1",
                            list(tbl=table)))
  if (length(factors)==0) return(rval[FALSE,])
  rval<-as.list(rval) 	## lists are faster
  if(is.character(factors)){
    for(f in factors){
      levs<- dbGetQuery(db,sqlsubst("select distinct %%v%% from %%tbl%% order by %%v%%",
                        list(v=f,tbl=table)))[[1]]
      rval[[f]]<-factor(rval[[f]],  levels=levs)
    }
    class(rval)<-"data.frame"
    return(rval[FALSE,])
  } else {
    ##numeric limit on levels
    if (!is.numeric(factors) || length(factors)>1)
        stop("invalid specification of 'factors'")
    if (factors<=0) return(rval[FALSE,])

    cat("checking factor levels\n")
    for(f in names(rval)){
      cat(".")
       levs<- dbGetQuery(db,
                        sqlsubst("select  %%v%% from %%tbl%% limit %%n%%",
                                 list(v=f, tbl=table, n=factors*10)))[[1]]
      
      if ((length(na.omit(unique(levs)))<=factors)){
        levs<- dbGetQuery(db,
                          sqlsubst("select distinct %%v%% from %%tbl%% order by %%v%% limit %%n%%",
                                   list(v=f, tbl=table, n=factors+2)))[[1]]
        if (length(na.omit(levs))<=factors)
          rval[[f]]<-factor(rval[[f]],  levels=levs)
      }
    }
    class(rval)<-"data.frame"
    return(rval[FALSE,])
  }
  
}

close.sqlsurvey<-function(con, ...){
  gc() ## try to make sure any dead model matrices and subsets are finalized.
  dbDisconnect(con$conn)
}

open.sqlsurvey<-function(con, driver, ...){  
  con$conn<-dbConnect(driver, url=con$dbname,...)
  if (!is.null(con$subset)){
    con$subset$conn<-con$conn
  }
  con
}


open.sqlmodelmatrix<-function(con, design,...){
  con$conn<-design$conn
  con
}

finalizeSubset<-function(e){
  dbSendUpdate(e$conn, sqlsubst("drop index %%idx%%",list(idx=e$idx)))
  dbSendUpdate(e$conn, sqlsubst("drop table %%tbl%%",list(tbl=e$table)))  
}


linbin2<-function(x,table,design,M=101,lim=NULL,y=NULL){
  if (M>800) stop("too many grid points")
  if (is.null(design$subset)){
    tablename<-table
    wtname<-design$weights
  } else {
    tablename<-sqlsubst(" %%tbl%% inner join %%subset%% using(%%key%%) ",
                        list(tbl=table, subset=design$subset$table, key=design$key))
    wtname<-design$subset$weights
  }
  if(is.null(lim)){
	xrange<-dbGetQuery(design$conn, sqlsubst("select min(%%x%%) as low, max(%%x%%) as up from %%tbl%%",
					list(x=x,tbl=tablename)))
	lim=c(xrange[1,"low"],xrange[1,"up"])
  }
  gridp<-seq(lim[1], lim[2], length=M)
  delta<-gridp[2]-gridp[1]

  bindata<-matrix(0,ncol=4,nrow=M+1)
  for(i in 2:M){
    qi<-sqlsubst("select sum(wt*(x-%%low%%)), sum((wt*(x-%%low%%)*y)),sum(wt*(%%hi%%-x)), sum((wt*(%%hi%%-x)*y)) from (select cast(%%xname%% as double) as x, cast(%%yname%% as double) as y, cast(%%wtname%% as double) as wt from %%table%%) as foo where x>%%low%% and x<=%%hi%%",list(low=gridp[i-1],hi=gridp[i],xname=x,yname=y,wtname=wtname,table=tablename)) 
    bindata[i,]<-as.numeric(as.matrix(dbGetQuery(design$conn, qi)))
  }
  bindata[is.na(bindata)]<-0
  
  N<-dbGetQuery(design$conn, sqlsubst("select sum(%%wt%%) from %%tbl%%", 
     list(wt=wtname,tbl=tablename)))[[1]]

  rval<-list(grid=gridp, xcounts=(bindata[2:(M+1),1]+bindata[1:M,3])/(delta), ycounts=(bindata[2:(M+1),2]+bindata[1:M,4])/(delta))
  rval
}

linbin1<-function(x,table,design,M=101,lim=NULL){
  if (M>800) stop("too many grid points")
  
  if (is.null(design$subset)){
    tablename<-table
    wtname<-design$weights
  } else {
    tablename<-sqlsubst(" %%tbl%% inner join %%subset%% using(%%key%%) ",
                        list(tbl=table, subset=design$subset$table, key=design$key))
    wtname<-design$subset$weights
  }
  
  if(is.null(lim)){
	xrange<-dbGetQuery(design$conn, sqlsubst("select min(%%x%%) as low, max(%%x%%) as up from %%tbl%%",
					list(x=x,tbl=tablename)))
	lim=c(xrange[1,"low"],xrange[1,"up"])
  }
  gridp<-seq(lim[1], lim[2], length=M)
  delta<-gridp[2]-gridp[1]

  bindata<-matrix(0,ncol=2,nrow=M+1)
  for(i in 2:M){
    qi<-sqlsubst("select sum(wt*(x-%%low%%)), sum(wt*(%%hi%%-x)) from (select cast(%%xname%% as double) as x,  cast(%%wtname%% as double) as wt from %%table%%) as foo where x>%%low%% and x<=%%hi%%",
                 list(low=gridp[i-1],hi=gridp[i],xname=x,wtname=wtname,table=tablename)) 
    bindata[i,]<-as.numeric(as.matrix(dbGetQuery(design$conn, qi)))
  }
  bindata[is.na(bindata)]<-0
  
  N<-dbGetQuery(design$conn, sqlsubst("select sum(%%wt%%) from %%tbl%%", 
     list(wt=wtname,tbl=tablename)))[[1]]

  rval<-list(grid=gridp, xcounts=(bindata[2:(M+1),1]+bindata[1:M,2]/(delta*N)))
  rval
}


svysmooth.sqlrepsurvey<-svysmooth.sqlsurvey<-function(formula, design, bandwidth=NULL, M=101, sample.bandwidth=NULL,...){

  if(!is.null(sample.bandwidth)){
    ## read a subsample to estimate the bandwidth
    if (is.null(design$subset)){
      tablename<-design$table
      wtname<-design$weights
    } else {
      tablename<-sqlsubst("%%tbl%% inner join %%subset%% using(%%key%%) ",
                          list(tbl=design$table, subset=design$subset$table, key=design$key))
      wtname<-design$subset$weights
    }

    
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

    
    if (!is.numeric(sample.bandwidth) || (sample.bandwidth<100) || (sample.bandwidth>1e5))
      stop("invalid sample.bandwidth")
    vars<- paste(all.vars(formula),collapse=",")
    mf <- model.frame(formula,data=dbGetQuery(design$conn, paste("select",vars, "from (select",vars,",",wtname,"from", tablename,"where (",wtname,">0) ) as foo sample",round(sample.bandwidth))))
    mm <- model.matrix(terms(formula), mf)
    if (attr(terms(formula), "intercept")) 
      mm <- mm[, -1, drop = FALSE]
    naa <- attr(mf, "na.action")
    if (length(formula) == 3) {
      Y <- mf[[1]]
      density <- FALSE
    }
    else density <- TRUE
    bandwidth <- numeric(ncol(mm))
    for (i in 1:ncol(mm)) {
      bandwidth[i] <- if (density) 
        dpik(mm[, i], gridsize = M)
      else dpill(mm[, i], Y, gridsize = M)
    }

    N<-dbGetQuery(design$conn, paste("select count(*) from",tablename,"where",wtname,">0"))
    bandwidth<-bandwidth*(sample.bandwidth/N)^(1/5)
  } else if (is.null(bandwidth)) stop("Must provide bandwidth or sample.bandwidth")
  
  
  if (length(formula)==2){
    x<-attr(terms(formula),"term.labels")
    if(length(x)>1) stop('only one variable')
    bins<-linbin1(x, table=design$table, design=design,M=M)
    rval<-list(locpoly(rep(1,M-1), bins$xcounts ,binned=TRUE,
                       bandwidth=bandwidth,range.x=range(bins$grid)))
    attr(rval,"ylab")<-"Density"
    names(rval)<-x
  } else {
    if(length(formula[[3]])>1) stop('only one variable')
    x<-attr(terms(formula),"term.labels")
    if(length(x)>1) stop('only one variable')
    y<-deparse(formula[[2]])
    bins<-linbin2(x, table=design$table, design=design,M=M,y=y)
    rval<-list(locpoly(bins$xcounts, bins$ycounts,binned=TRUE,bandwidth=bandwidth,
                       range.x=range(bins$grid)))
    names(rval)<-x
    attr(rval,"ylab")<-y
  }
  class(rval)<-"svysmooth"
  attr(rval,"call") <- sys.call()
  attr(rval,"density") <- (length(formula)==2)
  rval
}

sqlexpr<-function(expr, design){
   nms<-new.env(parent=emptyenv())
   assign("%in%"," IN ", nms)
   assign("&", " AND ", nms)
   assign("=="," = ",nms)
   assign("|"," OR ", nms)
   assign("!"," NOT ",nms)
   assign("I","",nms)
   assign("~","",nms)
   assign("(","",nms)
   out <-textConnection("str","w",local=TRUE)
   inorder<-function(e){
     if(length(e) ==1) {
       if (is.character(e))
         cat("'",e,"'",file=out,sep="")
       else
         cat(e, file=out)
     } else if (e[[1]]==quote(is.na)){
     	cat("(",file=out)
     	inorder(e[[2]])
     	cat(") IS NULL", file=out)
     } else if (length(e)==2){
       nm<-deparse(e[[1]])
       if (exists(nm, nms)) nm<-get(nm,nms)
       cat(nm, file=out)
       cat("(", file=out)
       inorder(e[[2]])
       cat(")", file=out)
     } else if (deparse(e[[1]])=="c"){
       cat("(", file=out)
       for(i in seq_len(length(e[-1]))) {
         if(i>1) cat(",", file=out)
         inorder(e[[i+1]])
       }
       cat(")", file=out)
     } else if (deparse(e[[1]])==":"){
       cat("(",file=out)
       cat(paste(eval(e),collapse=","),file=out)
       cat(")",file=out)
     } else{
         cat("(",file=out)
         inorder(e[[2]])
         nm<-deparse(e[[1]])
         if (exists(nm,nms)) nm<-get(nm,nms)
         cat(nm,file=out)
         inorder(e[[3]])
         cat(")",file=out)
       }

   }
   inorder(expr)
   close(out)
   paste("(",str,")")

}


sqlcutfn<-function (breaks) 
{
    n <- length(breaks)
    numbers<-format(1:(n+1))
    last <- NULL
    others <- NULL
    first <- paste("when x<=", breaks[1], " then return '",numbers[1],". to ", 
        breaks[1], "';", sep = "")
    if (n > 1) {
        last <- paste("when x>", breaks[n], " then return '", 
            numbers[n + 1], ". ", breaks[n], "+';", sep = "")
        others <- paste("when (x<=", breaks[-1], " and x>", breaks[-n], 
            ") then return '", numbers[2:n], ". ", breaks[-n], "+ to ", 
            breaks[-1], "';", sep = "")
    }
    whens <- paste(c(first, last, others), collapse = "\n")
    fname <- basename(tempfile("rec_"))
    query <- paste("create function", fname, "(x double precision)", 
        "returns varchar(255)", "begin", "case", whens, "else return null;", 
        "end case;", "end;", sep = "\n")
    tidy <- paste("drop function", fname)
    list(name = fname, construct = query, destroy = tidy)
}



  
cutprocess<-function(formula){

  functions<-list()
  
  descend<-function(e){
    if (length(e)==1){
      e
    } else if (e[[1]]==as.name("cut")){
      if(length(e)!=3) stop("cut(x,breaks) are the only supported arguments")
      fn<-sqlcutfn(eval(e[[3]],environment(formula)))
      functions<<-c(functions,list(fn))
      bquote(.(as.name(fn$name))(.(e[[2]])))      
    } else {
      for(i in 2:length(e)){
        e[[i]]<-descend(e[[i]])
      }
      e
    }
  }

  list(descend(formula),functions)
  
}

subset.sqlsurvey<-function(x,subset,...){

  subset<-substitute(subset)
  rval<-new.env()
  rval$subset<-sqlexpr(subset)

  rval$table<-basename(tempfile("_sbs_"))
  rval$idx<-basename(tempfile("_idx_"))
  rval$weights<-"_subset_weight_"

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
  
  if (is.null(x$subset)){
    query<-sqlsubst("create table %%tbl%% as (select %%key%%, %%wt%%*(1*%%subset%%) as _subset_weight_ from %%base%%) with data",
                    list(tbl=rval$table, key=x$key, wt=x$weights, subset=rval$subset, base=tablename )
                    )
    
  } else {
       query<-sqlsubst("create table %%tbl%% as (select %%key%%, (%%subsetwt%%*(1*%%subset%%)) as _subset_weight_ from  %%base%% inner join %%oldtbl%% using(%%key%%) ) with data",
                    list(tbl=rval$table, key=x$key, subset=rval$subset, base=tablename,oldtbl=x$subset$table,subsetwt=x$subset$weights)
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

sqlsurvey<-function(id, strata=NULL, weights=NULL, fpc="0",driver,
                       database, table.name=basename(tempfile("_tbl_")),
                       key, check.factors=10,...){


  db<-dbConnect(driver,database,...)

  if (is.data.frame(check.factors)){
    zdata<-check.factors
    actualnames<-dbListFields(db,table.name)
    if (!all(names(zdata) %in% actualnames)) stop("supplied data frame includes variables not in the data table")
    if (!all(actualnames %in% names(zdata))) message("levels for some variables not supplied: assumed numeric")
    for(v in setdiff(actualnames,names(zdata))) zdata[[v]]<-numeric(0)   
  } else{
    zdata<-make.zdata(db,table.name,factors=check.factors)
  }
  
  rval<-list(conn=db, table=table.name,
           id=id, strata=strata,weights=weights,fpc=fpc,
           call=sys.call(), zdata=zdata, key=key
           )

    rval$dbname<-database

  class(rval)<-"sqlsurvey"
  rval
}

sqlsubst<-function(strings, values){
  for(nm in names(values)){
  	if (is.null(values[[nm]])) next
    if (length(values[[nm]])>1) values[[nm]]<-paste(values[[nm]],collapse=", ")
    strings<-gsub(paste("%%",nm,"%%",sep=""),values[[nm]], strings)
  }
strings
}

print.sqlsurvey<-function(x,...){
  cat("MonetDB survey object (linearisation variances):\n")
  print(x$call)
  invisible(x)
}

dim.sqlsurvey<-function(x){
  if(is.null(x$subset))
    nrows<-dbGetQuery(x$conn, sqlsubst("select count(*) from %%table%%", list(table=x$table)))[[1]]
  else
    nrows<-dbGetQuery(x$conn, sqlsubst("select count(*) from %%table%% where %%wt%%>0",
                                       list(table=x$subset$table, wt=x$subset$weights)))[[1]]
  ncols<-ncol(x$zdata)
  c(nrows,ncols)
}


svymean.sqlsurvey<-function(x, design, na.rm=TRUE,byvar=NULL,se=FALSE, keep.estfun=FALSE,...){

  tms<-terms(x)

  ##handle subpopulations
  if (is.null(design$subset)){
    tablename<-design$table
    wtname<-design$weights
  } else {
    tablename<-sqlsubst(" %%tbl%% inner join %%subset%% using(%%key%%) ",
                        list(tbl=design$table, subset=design$subset$table, key=design$key))
    wtname<-design$subset$weights
  }

  updates<-NULL
  metavars<-NULL
  allv<-unique(c(all.vars(x),all.vars(byvar)))
  needsUpdates<-any(!sapply(allv,isBaseVariable,design=design))
  if (needsUpdates){
    metavars<-with(design,c(id,strata,wtname,fpc,key))
    updates<-getTableWithUpdates(design,allv,metavars,tablename)
    tablename<-updates$table
    on.exit(for (d in updates$destroyfns) dbSendUpdate(design$conn,d),add=TRUE)
    for(f in updates$createfns) dbSendUpdate(design$conn,f)
  }
  
  ## handle missing values
  for(v in allv){
    nmissing<-dbGetQuery(design$conn, sqlsubst("select sum(%%wt%%) from %%table%% where %%var%% is null",
                                               list(wt=wtname, table=tablename, var=v)))[[1]][1]
    if (!is.na(nmissing) && nmissing>0) break
  }
  if (!is.na(nmissing) && nmissing>0){
    if (na.rm==FALSE) return(NA)
    notna<-parse(text=paste(paste("!is.na(",allv,")"),collapse="&"))[[1]]
    design<-do.call(subset, list(design, notna))
    tablename<-sqlsubst(" %%tbl%% inner join %%subset%% using(%%key%%) ",
                        list(tbl=design$table, subset=design$subset$table, key=design$key))
    wtname<-design$subset$weights

    if (needsUpdates){
      metavars<-union(metavars,wtname)
      updates<-getTableWithUpdates(design,allv,metavars,tablename)
      tablename<-updates$table
    }
  }
  
  
  ## use sqlmodelmatrix if we have factors or interactions
  basevars<-rownames(attr(tms,"factors"))
  if ( any(attr(tms,"order")>1) || any(sapply(basevars, isFactorVariable, design=design))){
    mm<-sqlmodelmatrix(x,design, fullrank=FALSE)
    termnames<-mm$terms
    tablename<-sqlsubst("%%tbl%% inner join %%mm%% using(%%key%%)",
                        list(tbl=tablename,mm=mm$table, key=design$key))
  } else {
    termnames<-attr(tms,"term.labels")
  }
  vars<-paste("sum(",wtname,"*",termnames,")")
  tvars<-paste("sum(",wtname,"*(1*(",termnames," is not null)))")

  if (is.null(byvar)){
    query<-sqlsubst("select %%vars%%  from %%table%%",
                    list(vars=vars,table=tablename))
    tquery<-sqlsubst("select %%vars%% from %%table%%",
                    list(vars=tvars, table=tablename))
  }else{
    byvar<-attr(terms(byvar),"term.labels")
    query<-sqlsubst("select %%vars%%, %%byvars%% from %%table%% where %%wt%%<>0 group by %%byvars%% order by %%byvars%%",
                    list(vars=vars, byvars=byvar, wt=wtname, table=tablename))    
    tquery<-sqlsubst("select %%vars%%, %%byvars%% from %%table%% where %%wt%%<>0 group by %%byvars%% order by %%byvars%%",
                    list(vars=tvars, byvars=byvar, wt=wtname, table=tablename))    
  }
  result<-dbGetQuery(design$conn, query)
  p<-length(termnames)
  totwt<-dbGetQuery(design$conn, tquery)[1:p]
  result[1:p]<-result[1:p]/totwt

  names(result)<-c(termnames,byvar)
  if(se){
    utable<-basename(tempfile("_U_"))
    if (is.null(byvar)){
      means<-unlist(result[1,])
      unames<-paste("_",termnames,sep="")
      query<-sqlsubst("create table %%utbl%% as (select %%key%%, %%vars%% from %%table%%) with data",
                      list(utbl=utable, key=design$key,
                           vars=paste(termnames,"-",means," as ",unames,sep=""),
                           table=tablename))
    } else {
      means<-as.vector(t(as.matrix(result[,1:p,drop=FALSE])))
      bycols<-result[,-(1:p),drop=FALSE]
      qbycols<-lapply(bycols, function(v) if(is.character(v)) lapply(v,adquote) else v)
      bynames<-do.call(paste, c(mapply(paste, names(bycols),"=",qbycols,SIMPLIFY=FALSE), sep=" AND "))
      vnames<-paste("(",termnames,"-",means,")",sep="")
      uexpr<-paste(vnames,"*(1*(",rep(bynames,each=p),"))")
      unames<-paste("_",as.vector(outer(termnames, do.call(paste, c(bycols,sep="_")), paste,sep="_")),sep="")
      query<-sqlsubst("create table %%utbl%% as (select %%key%%, %%vars%% from %%table%%) with data",
                      list(utbl=utable, key=design$key,
                           vars=paste(uexpr,"as",unames),
                           table=tablename))
      
    }
    
    dbSendUpdate(design$conn, query)
    query<-sqlsubst("create unique index %%idx%% on  %%utbl%% (%%key%%)",
                    list(utbl=utable, key=design$key, idx=basename(tempfile("idx"))))
    dbSendUpdate(design$conn, query)
    if (!keep.estfun)
      on.exit(dbSendUpdate(design$conn, sqlsubst("drop table %%utbl%%",list(utbl=utable))),add=TRUE)
    vmat<-sqlvar(unames,utable,design)
    ##FIXME
    attr(result,"var")<-vmat/tcrossprod(as.vector(t(as.matrix(totwt))))

  }
  
  attr(result,"resultcol")<-1:length(termnames)
  class(result)<-c("sqlsvystat",class(result))
  result
  
}

coef.sqlsvystat<-function(object,...) as.vector(t(as.matrix(object[,attr(object,"resultcol"),drop=FALSE])))
vcov.sqlsvystat<-function(object,...) attr(object,"var")

print.sqlsvystat<-function(x,...){
  se<-SE(x)
  if (length(se))
    mat<-cbind(coef(x),SE=SE(x))
  else
    mat<-data.frame(coef(x))
  print(mat)
  invisible(x)
}

svytotal.sqlsurvey<-function(x, design, na.rm=TRUE,byvar=NULL,se=FALSE,keep.estfun=FALSE,...){

  design<-dropmissing(x,design,na.rm)
  
  tms<-terms(x)
  ## handle subpopulations
  if (is.null(design$subset)){
    tablename<-design$table
    wtname<-design$weights
  } else {
    tablename<-sqlsubst(" %%tbl%% inner join %%subset%% using(%%key%%) ",
                        list(tbl=design$table, subset=design$subset$table, key=design$key))
    wtname<-design$subset$weights
  }
  
  ## use modelmatrix if we have factors or interactions
  basevars<-rownames(attr(tms,"factors"))
  if (any(sapply(design$zdata[basevars], function(v) (length(levels(v))>0)|| is.character(v)))
      || any(attr(tms,"order")>1)){
    mm<-sqlmodelmatrix(x,design, fullrank=FALSE)
    termnames<-mm$terms
    tablename<-sqlsubst("%%tbl%% inner join %%mm%% using(%%key%%)",
                        list(tbl=tablename,mm=mm$table, key=design$key))
  } else {
    termnames<-attr(tms,"term.labels")
  }

  vars<-paste("sum(",wtname,"*(1*",termnames,"))",sep="")
  if (is.null(byvar)){
    query<-sqlsubst("select %%vars%% from %%table%%",
                    list(vars=vars, wt=wtname,table=tablename))
  }else{
    byvar<-attr(terms(byvar),"term.labels")
    query<-sqlsubst("select %%vars%%, %%byvars%%  from %%table%% group by %%byvars%% order by %%byvars%%",
                    list(vars=vars, byvars=byvar, wt=wtname, table=tablename))    
  }
  result<-dbGetQuery(design$conn, query)
  p<-length(termnames)
  names(result)<-c(termnames, byvar)

  if(se){
    utable<-basename(tempfile("_U_"))
    if (is.null(byvar)){
      unames<-paste("_",termnames,sep="")
      query<-sqlsubst("create table %%utbl%% as (select %%key%%, %%vars%% from %%table%%) with data",
                      list(utbl=utable, key=design$key,
                           vars=paste(termnames,"as",unames),
                           table=tablename))
    } else {
      bycols<-result[,-(1:p),drop=FALSE]
      qbycols<-lapply(bycols, function(v) if(is.character(v)) lapply(v,adquote) else v)
      bynames<-do.call(paste, c(mapply(paste, names(bycols),"=",qbycols,SIMPLIFY=FALSE), sep=" AND "))
      uexpr<-as.vector(outer(termnames, bynames, function(i,j) paste(i,"*(1*(",j,"))",sep="")))
      unames<-paste("_",as.vector(outer(termnames, do.call(paste, c(bycols,sep="_")), paste,sep="_")),sep="")
      query<-sqlsubst("create table %%utbl%% as (select %%key%%, %%vars%% from %%table%%)  with data",
                      list(utbl=utable, key=design$key,
                           vars=paste(uexpr,"as",unames),
                           table=tablename))
      
    }
    dbSendUpdate(design$conn, query)
    query<-sqlsubst("create unique index %%idx%% on  %%utbl%% (%%key%%)",
                    list(utbl=utable, key=design$key, idx=basename(tempfile("idx"))))
    dbSendUpdate(design$conn, query)
    if (!keep.estfun)
      on.exit(dbSendUpdate(design$conn, sqlsubst("drop table %%utbl%%",list(utbl=utable))))
    vmat<-sqlvar(unames,utable,design)
    attr(result,"var")<-vmat
    
  }
  
  attr(result,"resultcol")<-1:length(termnames)
  class(result)<-c("sqlsvystat",class(result))
  result
  
}


svyquantile.sqlsurvey<-function(x,design, quantiles,build.index=FALSE,...){
  SMALL<-10000 ## 20 for testing. More like 1000 for production use
  if (is.null(design$subset))
    tablename<-design$table
  else
    tablename<-sqlsubst("%%tbl%% inner join %%sub%% using(%%key%%)",
                        list(tbl=design$table, sub=design$subset$table,
                             key=design$key))
  bisect<-function(varname,wtname,  low, up, plow,pup, nlow,nup,quant,W){
    if (up==low) return(up)
    if (nup-nlow < SMALL){
      query<-sqlsubst("select %%var%%, %%wt%% from %%table%% where %%var%%>%%low%% and %%var%%<=%%up%%",
                      list(var=varname,wt=wtname,table=design$table,
                           low=low,up=up))
      data<-dbGetQuery(design$conn, query)
      return(bisect.in.mem(data, plow, pup,quant,W))
    }
    mid<-((pup-quant+0.5)*low+(quant-plow+0.5)*up)/(pup-plow+1)
    query<-sqlsubst("select sum(%%wt%%), count(*), max(%%var%%) from %%table%% where %%var%%<=%%mid%%",
                    list(var=varname,mid=mid,wt=wtname,table=design$table))
    result<-dbGetQuery(design$conn, query)
    mid<-result[[3]]
    nmid<-result[[2]]
    pmid<-result[[1]]/W
    if (mid==up && pmid>quant) return(mid)
    if (mid==low){
      query<-sqlsubst("select sum(%%wt%%)from %%table%% where %%var%% = %%mid%% ",
                      list(var=varname, mid=mid,table=design$table, wt=wtname))
      pexactmid<-dbGetQuery(design$conn,query)
      if (pmid+pexactmid>quant) return(up)
    }
    if (pmid>quant)
      bisect(varname,wtname,low,mid,plow,pmid,nlow,nmid,quant,W)
    else
      bisect(varname,wtname,mid,up,pmid, pup, nmid, nup, quant,W)
  }

  bisect.in.mem<-function(data, plow,pup,quant,W){
    data<-data[order(data[,1]),]
    p<-cumsum(data[,2])/W
    p<-p+plow
    p[length(p)]<-p[length(p)]+(1-pup)
    data[min(which(p>=quant)),1]
  }

  qsearch<-function(varname, quant){
    ll<-levels(design$zdata[[varname]])
    if (!is.null(ll) && length(ll)<100){
      tbl<-svytable(formula(paste("~",varname)),design)
      cdf<-cumsum(tbl[,2])/sum(tbl[,2])
      return(tbl[,1][min(which(cdf>=quant))])
    }
    if(build.index){
      idxname<-basename(tempfile("idx"))
      dbSendUpdate(design$conn, sqlsubst("create index %%idx%% on %%tbl%%(%%var%%)",
                                       list(idx=idxname, tbl=design$table,var=varname)))
      on.exit(dbSendUpdate(design$conn, sqlsubst("drop index %%idx%%",list(idx=idxname))))
    }
    lims<-dbGetQuery(design$conn,
                     sqlsubst("select min(%%var%%), max(%%var%%), count(*), sum(%%wt%%) from %%table%%",
                              list(var=varname, wt=design$weights, table=design$table)))
    bisect(varname,design$weights, lims[[1]],lims[[2]],0,1,0,lims[[3]],quant,lims[[4]])
  }

  if(length(quantiles)>1) stop("only one quantile")
  tms<-terms(x)
  rval<-sapply(attr(tms,"term.labels"), qsearch,quant=quantiles)
  names(rval)<-attr(tms,"term.labels")
  rval
}

dropmissing<-function(expr,design,na.rm){
   if (is.null(design$subset)){
    tablename<-design$table
    wtname<-design$weights
  } else {
    tablename<-sqlsubst(" %%tbl%% inner join %%subset%% using(%%key%%) ",
                        list(tbl=design$table, subset=design$subset$table, key=design$key))
    wtname<-design$subset$weights
  }

   for(v in all.vars(expr)){
     nmissing<-dbGetQuery(design$conn, sqlsubst("select sum(%%wt%%) from %%table%% where %%var%% is null", 
                                                list(wt=wtname, table=tablename, var=v)))[[1]][1]
     if (!is.na(nmissing) && nmissing>0) break
   }
   if (!is.na(nmissing) && nmissing>0){
     if (na.rm==FALSE) stop("missing values present and na.rm=FALSE")
     notna<-parse(text=paste(paste("!is.na(",all.vars(expr),")"),collapse="&"))[[1]]
     design<-do.call(subset, list(design, notna))
     tablename<-sqlsubst(" %%tbl%% inner join %%subset%% using(%%key%%) ",
                         list(tbl=design$table, subset=design$subset$table, key=design$key))
     wtname<-design$subset$weights
   }
   design
 }

svylm<-function(formula,design,...) UseMethod("svylm",design)

svylm.sqlsurvey<-function(formula, design,na.rm=TRUE,...){
  tms<-terms(formula)
  yname<-as.character(attr(tms,"variables")[[2]])

  design<-dropmissing(formula,design,na.rm)

  ## handle subpopulations
  if (is.null(design$subset)){
    tablename<-design$table
    wtname<-design$weights
  } else {
    tablename<-sqlsubst(" %%tbl%% inner join %%subset%% using(%%key%%) ",
                        list(tbl=design$table, subset=design$subset$table, key=design$key))
    wtname<-design$subset$weights
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
   qxwy <- sqlsubst("select %%sumxy%% from %%tablename%% inner join (select %%y%% as %%mfy%%, %%key%% from %%mf%%) as foo using(%%key%%)", 
        list(sumxy = sumxy, y = yname, key = design$key, tablename = tablename, 
        mf=mm$mf, wt=wtname, mfy=mfy))
   xwy<-drop(as.matrix(dbGetQuery(design$conn, qxwy)))
   beta<-solve(xwx,xwy)

   xytab<-basename(tempfile("_xyt_"))
   muname<-basename(tempfile("_mu_"))
   qmu<-paste("(",paste(termnames,"*",formatC(beta,format="fg",digits=16),collapse="+"),") as ",muname)
   qxytab<-sqlsubst("create table %%xytab%% as (select %%x%%, %%y%%, %%qmu%%, %%key%% from (select %%y%%, %%key%% from %%mf%%) as foo inner join %%mm%% using(%%key%%) ) with data",
                    list(xytab=xytab, x=termnames, y=yname, key=design$key,qmu=qmu,mf=mm$mf,mm=mm$table))       
   dbSendUpdate(design$conn, qxytab)
   on.exit(dbSendUpdate(design$conn, paste("drop table ",xytab)),add=TRUE)
   
   Utable<-basename(tempfile("_U_"))
   unames<-paste("_U_", termnames, sep="")
   u<-paste(termnames,"*(",yname,"-",muname,") as ",unames)
   qu<-sqlsubst("create table %%utable%% as (select %%u%%, %%key%% from %%xytab%%) with data",
   	list(key=design$key, utable=Utable, u=u,xytab=xytab))  
   dbSendUpdate(design$conn, qu)
   on.exit(dbSendUpdate(design$conn,paste("drop table ",Utable)),add=TRUE)

   Uvar<-sqlvar(unames,Utable, design)
   xwxinv<-solve(xwx)
  v<-xwxinv%*%Uvar%*%xwxinv
  names(beta)<-termnames
  results<-data.frame(beta=beta)
  
  dimnames(v)<-list(termnames, termnames)
  attr(results, "var")<-v
  
  attr(results,"resultcol")<-1:length(termnames)
  class(results)<-c("sqlsvystat",class(results))
   results
}

dim.sqlmm<-function(x){
	n<-dbGetQuery(x$conn, sqlsubst("select count(*) from %%table%%",list(table=x$table)))[[1]]
	p<-length(x$terms)
	c(n,p) 	
}

sqlvar<-function(U, utable, design){
   nstages<-length(design$id)
   units<-NULL
   strata<-NULL
   results<-vector("list",nstages)
   stagevar<-vector("list",nstages)
   p<-length(U)
   if (is.null(design$subset)){
    tablename<-design$table
    wtname<-design$weights
  } else {
    tablename<-sqlsubst(" %%tbl%% inner join %%subset%% using(%%key%%) ",
                        list(tbl=design$table, subset=design$subset$table, key=design$key))
    wtname<-design$subset$weights
  }
   sumUs<-sqlsubst(paste(paste("sum(",U,"*%%wt%%) as _s_",U,sep=""),collapse=", "),list(wt=wtname))
   Usums<-paste("_s_",U,sep="")
   avgs<-paste("avg(",Usums,")",sep="")
   avgsq<-outer(Usums,Usums, function(i,j) paste("avg(",i,"*",j,")",sep=""))
   for(stage in 1:nstages){
     oldstrata<-strata
     strata<-unique(c(units, design$strata[stage]))
     units<-unique(c(units, design$strata[stage], design$id[stage]))

     if(length(strata)>0){
       query<-sqlsubst("select %%avgs%%, %%avgsq%%, count(*) as _n_, %%fpc%% as _fpc_, %%strata%%
                      from 
                            (select %%strata%%, %%sumUs%%, %%fpc%% from %%basetable%% inner join %%tbl%% using(%%key%%) group by %%units%%)  as r_temp
                      group by %%strata%%" ,
                       list(units=units, strata=strata, sumUs=sumUs, tbl=utable,avgs=avgs,
                            avgsq=avgsq,fpc=design$fpc[stage], strata=strata,
                            basetable=tablename, key=design$key
                            )
                       )
     } else {
       query<-sqlsubst("select %%avgs%%, %%avgsq%%, count(*) as _n_, %%fpc%% as _fpc_
                      from 
                            (select  %%sumUs%%, %%fpc%% from %%basetable%% inner join %%tbl%% using(%%key%%) group by %%units%%)  as r_temp" ,
                       list(units=units, strata=strata, sumUs=sumUs, tbl=utable,avgs=avgs,
                            avgsq=avgsq,fpc=design$fpc[stage],
                            basetable=tablename, key=design$key
                            )
                       )
 
     }
     result<-dbGetQuery(design$conn, query)
     result<-subset(result, `_fpc_`!=`_n_`) ## remove certainty units
     if (is.null(oldstrata)){
       result$`_p_samp_`<-1
     } else {
       index<-match(result[,oldstrata], results[[stage-1]][,oldstrata])
       keep<-!is.na(index)
       result<-result[keep,,drop=FALSE]
       index<-index[keep]
       result$`_p_samp_`<-results[[stage-1]][index,"_n_"]/results[[stage-1]][index,"_fpc_"] ##assumes p = n/N (missing data?)
     }
     means<-as.matrix(result[,1:p])
     ssp<-as.matrix(result[,p+(1:(p*p))])
     meansq<-means[,rep(1:p,p)]*means[,rep(1:p,each=p)]
     nminus1<-(result$`_n_`-1)
     if (any(nminus1==0)){
       if (getOption("survey.lonely.psu")=="remove")
         nminus1[nminus1==0]<-Inf
       else
         stop("strata with only one PSU at stage ",stage)
     }
     stagevar[[stage]]<-((ssp-meansq) * (result$`_n_`^2)/nminus1)*result$`_p_samp_`

     if (any(result$`_fpc_`>0)) {## without-replacement
       stagevar[[stage]][result$`_fpc_`>0]<-stagevar[[stage]][result$`_fpc_`>0]*((result$`_fpc_`-result$`_n_`)/result$`_fpc_`)[result$`_fpc_`>0]
     }
     results[[stage]]<-result
 }
   vars<-lapply(stagevar, function(v) matrix(colSums(v),p,p))
   rval<-vars[[1]]
   for(i in seq(length=nstages-1)) rval<-rval+vars[[i+1]]
   dimnames(rval)<-list(U,U)
   rval
}

svytable.sqlsurvey<-function(formula, design,...){
  tms<-terms(formula)
  if (is.null(design$subset))
    tablename<-design$table
  else
    tablename<-sqlsubst("%%base%% inner join %%sub%% using(%%key%%) where %%wt%%>0",
                        list(base=design$table, sub=design$subset$table,
                             key=design$key, wt=design$subset$weights))
  query<-sqlsubst("select %%tms%%, sum(%%wt%%) from %%table%% group by %%tms%%",
                  list(wt=design$weights,
                       table=tablename,
                       tms=attr(tms,"term.labels")))
  dbGetQuery(design$conn, query)
}


adquote<-function(s) paste("\'",s,"\'",sep="")

sqlmodelmatrix<-function(formula, design, fullrank=TRUE){
  mmcol<-function(variables,levels, name.only=FALSE){
    if (length(variables)==0){
      if(name.only) return("_Intercept_") else return("1 as _Intercept_")
    }
    rval<-paste("(1*(",variables,"=",adquote(levels),"))",sep="")
    termname<-paste(variables,levels,sep="")
    if (length(rval)>1){
      rval<-paste(paste("(",rval,")",sep=""),collapse="*")
      termname<-paste(termname,collapse="_")
    }
    if (name.only)
      make.db.names(design$conn, termname)
    else 
      paste(rval,"as",make.db.names(design$conn, termname))
  }
  
  if (!all(sapply(all.vars(formula), isKnownVariable, design=design)))
    stop("some variables not in database")
  ok.names<-c("~","I","(","-","+","*")
  if (!all( all.names(formula) %in% c(ok.names, all.vars(formula))))
    stop("Unsupported transformations in formula")



  mftable<-basename(tempfile("_mf_"))
  mmtable<-basename(tempfile("_mm_"))

  ##FIXME needs to use getTableWithUpdates
  dbSendUpdate(design$conn, sqlsubst("create table %%mf%% as (select %%key%%, %%vars%% from %%table%%) with data",
                               list(mf=mftable, vars=all.vars(formula), table=design$table,
                                    id=design$id, strata=design$strata,key=design$key)))
  dbSendUpdate(design$conn, sqlsubst("create unique index %%idx%% on %%tbl%%(%%key%%)",
                                   list(idx=basename(tempfile("idx")), tbl=mftable,
                                        key=design$key)))
  
  tms<-terms(formula)
  mf<-zero.model.frame(formula,design)
 
  ## character variables get temporary factorness
  for(v in all.vars(formula)){
    if (is.character(mf[[v]])){
      charlevels<-dbGetQuery(design$conn, paste("select distinct",v,"from",mftable))[[1]]
      mf[[v]]<-factor(mf[[v]],levels=charlevels)
    }   
  }

  mm<-model.matrix(tms, mf)
  ntms<-max(attr(mm,"assign"))


  patmat<-attr(tms,"factors")
  nms<-attr(tms,"term.labels")
  orders<-attr(tms, "order")
  if (fullrank)
    contrastlevels<-function(f) {levels(f)[-1]}
  else
    contrastlevels<-levels

  
  mmterms<-lapply(1:ntms,
                    function(i){
                      vars<-rownames(patmat)[as.logical(patmat[,nms[i]])]
                      if (orders[i]==1 && is.null(levels(mf[[vars]])))
                        return(list(paste(vars," as _",vars,sep="")))
                      levs<-as.matrix(expand.grid(lapply(mf[vars],contrastlevels)))
                      lapply(split(levs,row(levs)),
                             function(ll) mmcol(vars,ll))
                    })
  if (fullrank)  mmterms<-c(mmterms, list(mmcol(NULL,NULL)))
  
  mmnames<-lapply(1:ntms,
                    function(i){
                      vars<-rownames(patmat)[as.logical(patmat[,nms[i]])]
                      if (orders[i]==1 && is.null(levels(mf[[vars]])))
                        return(list(paste("_",vars,sep="")))
                      levs<-as.matrix(expand.grid(lapply(mf[vars],contrastlevels)))
                      lapply(split(levs,row(levs)),
                             function(ll) mmcol(vars,ll,TRUE))
                    })
  if (fullrank) mmnames<-c(mmnames, list(mmcol(NULL,NULL,TRUE)))

  
  mmquery<-sqlsubst("create table %%mm%% as (select %%key%%, %%terms%% from %%mf%%) with data",
                    list(mm=mmtable, id=design$id, strata=design$strata,
                         terms=unlist(mmterms), mf=mftable,
                         key=design$key))
  dbSendUpdate(design$conn, mmquery)
  dbSendUpdate(design$conn, sqlsubst("create unique index %%idx%% on %%tbl%%(%%key%%)",
                                   list(idx=basename(tempfile("idx")), tbl=mmtable,
                                        key=design$key)))
  rval<-new.env(parent=emptyenv())
  rval$table<-mmtable
  rval$mf<-mftable
  rval$formula<-formula
  rval$terms<-unlist(mmnames)
  rval$call<-sys.call()
  rval$conn<-design$conn
  reg.finalizer(rval, sqlmmDrop)
  class(rval)<-"sqlmm"
  rval
}

sqlmmDrop<-function(mmobj){
  dbSendUpdate(mmobj$conn,sqlsubst("drop table %%mm%%", list(mm=mmobj$table))) 
  dbSendUpdate(mmobj$conn,sqlsubst("drop table %%mf%%", list(mf=mmobj$mf)))
  invisible(NULL)
}

head.sqlmm<-function(x,n=6,...) dbGetQuery(x$conn,
     sqlsubst("select * from %%mm%% limit %%nn%%", list(mm=x$table,nn=n)))

hexbinmerge<-function(h1,h2){
	h<-h1
	h@cID<-NULL
	
	extra<-!(h2@cell %in% h1@cell)
	cells<-sort(unique(c(h1@cell, h2@cell)))
	i1<-match(h1@cell,cells)
	i2<-match(h2@cell,cells)
	i2new<-match(h2@cell[extra], cells)
	i2old<-match(h2@cell[!extra], cells)
	
	n<-length(cells)

	count<-integer(n)
	count[i1]<-h1@count
	count[i2new]<-h2@count[extra]
	count[i2old]<-count[i2old]+h2@count[!extra]
	h@count<-count
		
	xcm<-numeric(n)
	xcm[i1]<-h1@xcm
	xcm[i2new]<-h2@xcm[extra]
	xcm[i2old]<-xcm[i2old]*(1-h2@count[!extra]/count[i2old])+h2@xcm[!extra]*(h2@count[!extra]/count[i2old])
	h@xcm<-xcm
	
	ycm<-numeric(n)
	ycm[i1]<-h1@ycm
	ycm[i2new]<-h2@ycm[extra]
	ycm[i2old]<-ycm[i2old]*(1-h2@count[!extra]/count[i2old])+h2@ycm[!extra]*(h2@count[!extra]/count[i2old])
	h@ycm<-ycm
	
	h@n<-as.integer(sum(count))
	h@ncells<-n	
	h@cell<-cells
	h
	}

sqlhexbin<-function(formula, design, xlab=NULL,ylab=NULL, ...,chunksize=10000){
	require("hexbin")
	tms<-terms(formula)
	x<-attr(tms,"term.labels")
	if (length(x)>2) stop("only one x variable")
	y<-deparse(formula[[2]])
	if (is.null(design$subset)){
   		tablename<-design$table
    	wtname<-design$weights
  	} else {
    	tablename<-sqlsubst(" %%tbl%% inner join %%subset%% using(%%key%%) ",
                        list(tbl=design$table, subset=design$subset$table, key=design$key))
    	wtname<-design$subset$weights
  	}

	query<-sqlsubst("select min(%%x%%) as xmin, max(%%x%%) as xmax, min(%%y%%) as ymin, max(%%y%%) as ymax from %%tbl%% where %%wt%%>0",
                        list(x=x,tbl=tablename,y=y, wt=wtname))
	ranges<-dbGetQuery(design$conn, query)
	xlim<-with(ranges,c(xmin,xmax))
	ylim<-with(ranges,c(ymin,ymax))
	N<-dbGetQuery(design$conn, sqlsubst("select count(*) from %%tbl%% where %%wt%%>0",
	   list(tbl=tablename,wt=wtname)))[[1]]
	
	query<-sqlsubst("select %%x%% as x, %%y%% as y, %%wt%% as _wt from %%tbl%% where %%wt%% > 0",
		list(x=x,y=y,tbl=tablename,wt=wtname))
	result<-dbSendQuery(design$conn, query)
	on.exit(dbClearResult(result))

    got<-0
    htotal<-NULL
    while(got<N){
    	df<-fetch(result, chunksize)
    	if (nrow(df)==0) break
    	h<-hexbin(df$x,df$y,IDs=TRUE,xbnds=xlim,ybnds=ylim)
    	h@count<-as.vector(tapply(df[["_wt"]],h@cID,sum))
    	if (is.null(htotal)){
    		htotal<-h
    	} else 
    		htotal<-hexbinmerge(htotal,h) 
    	
    }
    if (is.null(xlab)) xlab<-x
    if (is.null(ylab)) ylab<-y
    
    gplot.hexbin(htotal,xlab=xlab, ylab=ylab, ...)
    invisible(htotal)
	}


svyplot.sqlsurvey<-svyplot.sqlrepsurvey<-function (formula, design, style = c("hex", "grayhex","subsample"), ...) 
{
  style <- match.arg(style)
  mf<-match.call()
  mf[[1]]<-switch(style, hex = as.name("sqlhexbin"), grayhex=as.name("sqlhexbin"),subsample = as.name("sqlscatter"))
  mf$style<-switch(style, hex="centroids",grayhex=NULL,subsample=NULL)
  invisible(eval(mf,parent.frame()))
}


sqlscatter<-function(formula,design,npoints=1000,nplots=4,...){
  if (is.null(design$subset)){
    tablename<-design$table
    wtname<-design$weights
  } else {
    tablename<-sqlsubst(" %%tbl%% inner join %%subset%% using(%%key%%) ",
                        list(tbl=design$table, subset=design$subset$table, key=design$key))
    wtname<-design$subset$weights
  }
  
  n<-round(10*sqrt(nrow(design)))
  
  vars<-union(all.vars(formula),all.vars(substitute(list(...))))
  samp<-dbGetQuery(design$conn, sqlsubst("select %%wt%%,%%vars%% from %%table%% sample %%n%%",list(vars=vars,wt=wtname, table=tablename,n=n)))
  
  m<-match.call(expand.dots=FALSE)
  for(i in 1:nplots){
    idx<-sample(1:n,npoints,prob=samp[,1], replace=TRUE)
    subsamp<-samp[idx,]
    dots<-lapply(m$...,eval,subsamp,parent.frame())
    do.call(plot, c(list(formula=formula, data=samp[idx,]),dots))
  }
  invisible(NULL)
}
  


dim.sqlsurvey<-function(x){
  if(is.null(x$subset))
    nrows<-dbGetQuery(x$conn, sqlsubst("select count(*) from %%table%%", list(table=x$table)))[[1]]
  else
    nrows<-dbGetQuery(x$conn, sqlsubst("select count(*) from %%subtable%%",
                                       list(subtable=x$subset$table)))[[1]]
  ncols<-length(allVarNames(x))
  c(nrows,ncols)
}

dimnames.sqlsurvey<-function(x,...) list(character(0),allVarNames(x))


