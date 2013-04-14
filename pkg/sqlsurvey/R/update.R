
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



update.sqlsurvey<-function(object, ..., MAX.LEVELS=100){
  dots <- substitute(list(...))[-1]
  newnames <- names(dots)

  updates<-mapply(function(name,dot) makeUpdateVar(object,name, dot,max.levels=MAX.LEVELS), newnames, dots, SIMPLIFY=FALSE)

  if (is.null(object$updates))
    object$updates<-list(updates)
  else{
    warning("multiple updates not tested yet")
    object$updates<-c(object$updates, list(updates))
  }
  object
}


update.sqlrepsurvey<-function(object, ..., MAX.LEVELS=100){
  dots <- substitute(list(...))[-1]
  newnames <- names(dots)

  updates<-mapply(function(name,dot) makeUpdateVar(object,name, dot,max.levels=MAX.LEVELS), newnames, dots, SIMPLIFY=FALSE)

  if (is.null(object$updates))
    object$updates<-list(updates)
  else{
    object$updates<-c(object$updates, list(updates))
  }
  object
}


getOriginalInputs<-function(input, design){
  if(sqlsurvey:::isCreatedVariable(input,design)){
    for(i in seq_along(design$updates)){
      if (input %in% names(design$updates[[i]])){
      	return(do.call(c,lapply(design$updates[[i]][[input]]$inputs, getOriginalInputs,design=design)))
        }
    }
  } else input

}

makeUpdateVar<-function(design, varname, expr,max.levels){
   fname<-basename(tempfile(varname))
   inputs<-all.vars(expr)

   if(usesCreated<-any(sapply(inputs, isCreatedVariable,design=design))) {

     stop("can't (yet) use created variables as inputs")
     oldinputs<-inputs
     inputs<-unique(do.call(c,lapply(inputs,getOriginalInputs,design=design)))
     }
   
   inputtypes<-sapply(inputs, sqltype, design=design)

   fnexpr<-sqlexpr(expr,design)
   fnbody<-paste("return",fnexpr,";")

   if (usesCreated){
     ## declare inputs
     ## set input = inputfunction(original,inputs)
     ## fnexpr
   }
   
   outtest<-dbSendQuery(design$conn, paste("select (",fnexpr,") as tmp from",design$table,"limit 1"))
   sqlouttype<-as.character(dbColumnInfo(outtest)[1,2])
   Routtype<-as.character(dbColumnInfo(outtest)[1,3])
   dbClearResult(outtest)

   if (Routtype=="character"){
     levs<-dbGetQuery(design$conn, paste("select distinct (",fnexpr,") as ",varname,"from",design$table, "limit",max.levels+2))
     if (nrow(levs)<=max.levels){
       Routtype<-"factor"
       zf<-list(factor(integer(0),levels=levs[[1]]))
       names(zf)<-varname
     } else {
       zf<-list(character(0))
       names(zf)<-varname
     }
   } else {
     zf<-list(numeric(0))
     names(zf)<-varname
   }
   
   typedec<-paste("(",paste( paste(inputs, inputtypes),collapse=","),")")
   retdec<-paste("returns",sqlouttype)
   qcons<-  paste("create function", fname, typedec, retdec,
         "begin", fnbody, "end;", sep = "\n")
   qdestroy <- paste("drop function", fname)
   quse<-paste(fname,"(",paste(inputs,collapse=", "),") as ",varname,sep="")
   
   list(varname=varname,inputs=inputs,fname=fname,qcons=qcons,quse=quse,qdestroy=qdestroy,returns=Routtype,zdata=zf)
}

getTableWithUpdates<-function(design, vars, metavars, table){

  if (any(!sapply(vars,isKnownVariable,design=design))) stop("variable not found")
  basevars<-vars[vars %in% names(design$zdata)]
  updatevars<-vars[!(vars %in% names(design$zdata))]
  selectvars<-sapply(updatevars, function(v) {for (i in seq_along(design$updates)) if (v %in% names(design$updates[[i]])) return(design$updates[[i]][[v]]$quse)})
  createvars<-sapply(updatevars, function(v) {for (i in seq_along(design$updates)) if (v %in% names(design$updates[[i]])) return(design$updates[[i]][[v]]$qcons)})
  destroyvars<-sapply(updatevars, function(v) {for (i in seq_along(design$updates)) if (v %in% names(design$updates[[i]])) return(design$updates[[i]][[v]]$qdestroy)})
  fnames<-sapply(updatevars, function(v) {for (i in seq_along(design$updates)) if (v %in% names(design$updates[[i]])) return(design$updates[[i]][[v]]$fname)})

  select<-paste( "(select", paste(c(basevars,selectvars,metavars), collapse=", "), "from", table,") as thingy")
  list(createfns=createvars,destroyfns=destroyvars,table=select,fnames=fnames)
                    
}


isKnownVariable<-function(varname, design) {
  if (varname %in% names(design$zdata))
    TRUE
  else if (is.null(design$updates))
    FALSE
  else
    any(sapply(design$updates, function(x) varname %in% names(x)))
             
}

isBaseVariable<-function(varname, design){
  varname %in% names(design$zdata)
}


isCreatedVariable<-function(varname, design){
  !isBaseVariable(varname,design) && isKnownVariable(varname,design)
}

isFactorVariable<-function(varname, design){
 if (varname %in% names(design$zdata))
    return(is.factor(design$zdata[[varname]]) || is.character(design$zdata[[varname]]))

 n<-length(design$updates)
 if (n==0) stop("this can't happen")
 for(i in n:1){
   if (varname %in% names(design$updates[[i]])) return(design$updates[[i]][[varname]]$returns %in% c("character","factor"))
 }
 stop(paste(varname,"not found"))
}

getZData<-function(varname,design){
  if(isBaseVariable(varname,design))
    return(design$zdata[[varname]])
  else{
    n<-length(design$updates)
    if (n==0) stop("this can't happen")
    for(i in n:1){
      if (varname %in% names(design$updates[[i]])) return(design$updates[[i]][[varname]]$zdata )
    }
  }
}

zero.model.frame<-function(formula,design){
  ## assumes all variables are actually available: caller must check
  allv<-all.vars(formula)
  if (all(base<-sapply(allv,isBaseVariable,design=design)))
    return(model.frame(make.formula(allv),design$zdata,na.action=na.pass))

  zf<-design$zdata
  needed<-allv[!base]
  for(v in needed)
    zf<-cbind(zf,getZData(v,design))
  
  model.frame(make.formula(allv),zf,na.action=na.pass)
  
}

allVarNames<-function(design) {
  basenames<-names(design$zdata)
  if(is.null(design$updates)){
    basenames
  } else {
    c(basenames, do.call(c, lapply(design$updates, names)))
  }

}
