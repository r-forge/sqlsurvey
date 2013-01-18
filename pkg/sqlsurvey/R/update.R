
sqltype<-function(varname,design){
  if (varname %in% names(design$zdata)){
    rtype<-class(design$zdata[[varname]])
    switch(rtype,integer="int",numeric="double",factor=paste("varchar(",max(nchar(levels(design$zdata[[varname]])))+1,")"), character="varchar(255)")
  } else {
    n<-length(design$updates)
    if (n>0){
      for(i in n:1){
        if (varname %in% names(design$updates[[n]]))
          return( design$updates[[n]][[varname]]$outtype )
      }
    }
    stop(paste("variable",varname,"not found"))
  }

}



update.sqlsurvey<-function(object, ...){
  dots <- substitute(list(...))[-1]
  newnames <- names(dots)

  updates<-mapply(function(name,dot) makeUpdateVar(object,name, dot), newnames, dots, SIMPLIFY=FALSE)

  if (is.null(object$updates))
    object$updates<-list(updates)
  else{
    stop("multiple updates not implemented yet")
    ##object$updates<-c(object$updates, list(updates))
  }
  object
}


makeUpdateVar<-function(design, varname, expr){
   fname<-basename(tempfile(varname))
   inputs<-all.vars(expr)
   inputtypes<-sapply(inputs, sqltype, design=design)

   
   fnexpr<-sqlexpr(expr,design)
   fnbody<-paste("return",fnexpr,";")

   outtest<-dbSendQuery(design$conn, paste("select (",fnexpr,") as tmp from",design$table,"limit 1"))
   sqlouttype<-as.character(dbColumnInfo(outtest)[1,2])
   Routtype<-as.character(dbColumnInfo(outtest)[1,3])
   dbClearResult(outtest)
   
   typedec<-paste("(",paste( paste(inputs, inputtypes),collapse=","),")")
   retdec<-paste("returns",sqlouttype)
   qcons<-  paste("create function", fname, typedec, retdec,
         "begin", fnbody, "end;", sep = "\n")
   qdestroy <- paste("drop function", fname)
   quse<-paste(fname,"(",paste(inputs,collapse=", "),") as ",varname,sep="")
   
   list(varname=varname,inputs=inputs,fname=fname,qcons=qcons,quse=quse,qdestroy=qdestroy,returns=Routtype)
}

getTableWithUpdates<-function(design, vars, metavars, table){

  basevars<-vars[vars %in% names(design$zdata)]
  updatevars<-vars[!(vars %in% names(design$zdata))]
  selectvars<-sapply(updatevars, function(v) {if (v %in% names(design$updates[[1]])) design$updates[[1]][[v]]$quse else stop(paste(v,'not found'))})
  createvars<-sapply(updatevars, function(v) {if (v %in% names(design$updates[[1]])) design$updates[[1]][[v]]$qcons})
  destroyvars<-sapply(updatevars, function(v) {if (v %in% names(design$updates[[1]])) design$updates[[1]][[v]]$qdestroy})
  fnames<-sapply(updatevars, function(v) {if (v %in% names(design$updates[[1]])) design$updates[[1]][[v]]$fname})

  select<-paste( "(select", paste(c(basevars,selectvars,metavars), collapse=", "), "from", table,") as thingy")
  list(createfns=createvars,destroyfns=destroyvars,table=select,fnames=fnames)
                    
}


