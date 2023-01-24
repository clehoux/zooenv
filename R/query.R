

#' SQL query and summary of environmental variables using SGDO
#'
#' @param latitude vector of latitude please remove na from data frame prior to function
#' @param longitude  vector of longitude, please remove na from data frame prior to function
#' @param date vector of date format year month day
#' @param time vector of time format HH:MM
#' @param timezone vector of time zone, please use time zone recongnizable by R
#' @param depth.max vector of depth of station. for now it is requisited but may become optional in the future
#' @param ID  unique identifier of stations/rows.
#' @param geotol geographic tolerance for searching profiles near the station. Default to 0.1 degrees.
#' @param timetol time tolerance for searching profiles near the station, default to 24
#' @param station consecutive identifier if available, facultative. If given, the CTD in the search radius that has the same consecutive as the station is selected.
#' @param range_ctd vector of 2 values, identifying the minimum and maximum range for summarizing temperature and salinity
#' @param range_bot vector of 2 values, for chlrophyll integration
#' @param drv driver use this : DBI::dbDriver("Oracle")
#'
#' @return a data frame  with the unique idnetifier and calculated variables, join with your data using the unique identifier
#' @export
#'
#' @examples
#' #df <-  data.frame(latitude=47.17133,	longitude=-63.7465, time=20:33, timezone=America/Halifax, depth.max=56, ID=1, consecutive=72)
#' #SGDO_sql(latitude=df$latitude, longitude=df$longitude, date=df$date, time=df$time, timezone=df$timezone,depth.max=df$depth.max, ID=df$ID, geotol=0.01, timetol=6, station=df$consecutive)
#'
#'
#'
SGDO_sql<-function(drv, latitude, longitude, date, time, timezone="UTC",depth.max=Inf, ID, geotol=0.02, timetol=24, station=NULL, range_ctd=c(0,50), range_bot=c(0,100)){

  #verifiy conditions
  if(anyNA(latitude)) {stop("removes rows with missing positions")}
  if(anyNA(longitude)) {stop("removes rows with missing positions")}
  if(anyNA(date)){stop("removes rows with missing dates")}

  if(is.null(ID)) ID = 1:length(latitude)

  if(length(latitude)!=length(longitude) | length(longitude)!= length(date) | length(date) != length(time)| length(time)!=length(ID)) stop("data vectors of unequal size")

     # create an Oracle Database instance and create connection

  host=rstudioapi::askForPassword("Enter adress for host")
    # connect string specifications
  connect.string <- paste(
    "(DESCRIPTION=",
    "(ADDRESS=(PROTOCOL=tcp)(HOST=",host,")(PORT=1521))",
    "(CONNECT_DATA=(SERVICE_NAME =SGDOP)))", sep = "")


  # use username/password authentication
  #create empty data.frame that will include the dataset and ctd info
  ctd_id<-data.frame()

  pb <- txtProgressBar(min = 0,      # Minimum value of the progress bar
                       max = length(ID), # Maximum value of the progress bar
                       style = 3,    # Progress bar style (also available style = 1 and style = 2)
                       width = 50,   # Progress bar width. Defaults to getOption("width")
                       char = "=")

  username=rstudioapi::askForPassword("Enter your username")
  password=rstudioapi::askForPassword("Enter your password")
  #loop to extract ctd for each observations
  for(i in 1:length(ID)){
    conn <- ROracle::dbConnect(drv, username=username, password=password, dbname = connect.string)
if(nchar (date[i])!=10) stop("Date format is not OK. should be YYYY/MM/DD")

    setTxtProgressBar(pb, i)
      sample_date <- lubridate::ymd_hms(paste0(date[i], time[i],":00"), tz=timezone[i])
      sample_date <-  lubridate::with_tz(sample_date, tz="UTC")
      d<- lubridate::dhours(x = timetol)
      start_date<-sample_date-d
      end_date<-sample_date+d
      sample_date<-gsub(sample_date,pattern="-", replacement="/")#change format to fit with sql
      start_date<-gsub(start_date,pattern="-", replacement="/")#change format to fit with sql
      end_date<-gsub(end_date,pattern="-", replacement="/")#change format to fit with sql


    sample_lat<-latitude[i]
    start_lat<-latitude[i]-geotol  #geographic query within 1 degree lat/long
    end_lat<-latitude[i]+geotol

    sample_lon<-longitude[i]
    start_lon<-longitude[i]-geotol
    end_lon<-longitude[i]+geotol


    ###xbt###
    sql_xbt<-paste0("SELECT DISTINCT
                xbt.seq_jd,
                xbt.latd,
                xbt.lond,
                xbt.sytm,
                jd.COT_QUAL_JD,
                jd.PROFD_MAX_JD,
                jd.stat_jd AS Station_ctd,
                ABS(xbt.latd -", sample_lat ,") AS latdiff,
                ABS(",sample_lon ," - xbt.lond) AS longdiff,
                mi.desc_miss,
                mi.acr_miss,
                TO_CHAR(mi.dat_deb_miss,'YYYY') AS year
                FROM SGDO.DONNEES_XBT xbt, SGDO.JEU_DONNEES jd, SGDO.MISSION_EXP mi
                WHERE
                jd.seq_jd = xbt.seq_jd
                AND mi.acr_miss=jd.acr_miss
                AND(xbt.LATD BETWEEN ", start_lat ," AND ",end_lat," AND xbt.LOND BETWEEN ", start_lon ," AND ", end_lon ,")
                AND TO_CHAR(xbt.sytm, 'YYYY/MM/DD HH24:MI:SS') >= '",start_date,"'
                AND TO_CHAR(xbt.sytm, 'YYYY/MM/DD HH24:MI:SS') <= '",end_date, "'
                AND xbt.TE90 IS NOT NULL")

    # NUMTODSINTERVAL(TO_DATE(sg.sytm, 'YYYY/MM/DD HH24:MI:SS') - TO_DATE ('",sample_date,"' , 'YYYY/MM/DD HH24:MI:SS'), 'SECOND') as daysdifference,
    #remove from query, caused problems
    rsxbt <- ROracle::dbSendQuery(conn,sql_xbt)
    xbresults<-ROracle::fetch(rsxbt)

    if(!is.null(station) & nrow(xbresults >1)){
      if(nrow(xbresults)>1 & station[i] %in% xbresults$STATION_CTD) xbresults<-  xbresults %>% dplyr::filter(STATION_CTD==station[i])
      xbt_JD=xbresults$SEQ_JD
    }

    if(is.null(station) | nrow(xbresults)>1){

      xbt_sf <- sf::st_as_sf(xbresults, coords = c("LOND", "LATD"))
      sf::st_crs(xbt_sf) <- 4326

      dat_sf <- sf::st_as_sf(as.data.frame(cbind(latitude=latitude[i],longitude= longitude[i])), coords = c("longitude", "latitude"))
      sf::st_crs(dat_sf) <- 4326
      #cal_sf<-st_transform(cal_sf, crs=lcc)

      #dustance is in meters does not work with lat long 0.2
      xbt_JD= suppressMessages(sf::st_join(dat_sf, xbt_sf, join = nngeo::st_nn)$SEQ_JD)
      xbresults <- xbresults %>%  dplyr::filter(SEQ_JD==xbt_JD)

    }

    if(nrow(xbresults ==1)){
      xbt_JD=xbresults$SEQ_JD
    }

    if(nrow(xbresults !=0)){


      sql_xbt2<-paste0("SELECT
                xbt.deph,
                xbt.te90,
                jd.PROFD_MAX_JD
                FROM SGDO.DONNEES_XBT xbt,
                SGDO.JEU_DONNEES jd
                WHERE
               xbt.SEQ_JD = jd.SEQ_JD
                AND xbt.seq_jd IN('",xbt_JD,"')")
      rsxbt2 <- ROracle::dbSendQuery(conn,sql_xbt2)
      xbresults2<-ROracle::fetch(rsxbt2)
      xbresults2$ID <- ID[i]
    }


    if(nrow(xbresults) ==0) xbresults2 <-  data.frame(ID=ID[i])


    ####ctd####
    sql_ctd<-paste0("SELECT DISTINCT
                sg.seq_jd,
                sg.latd,
                sg.lond,
                sg.sytm,
                jd.COT_QUAL_JD,
                jd.PROFD_MAX_JD,
                jd.stat_jd AS Station_ctd,
                ABS(sg.latd -", sample_lat ,") AS latdiff,
                ABS(",sample_lon ," - sg.lond) AS longdiff,
                mi.desc_miss,
                mi.acr_miss,
                TO_CHAR(mi.dat_deb_miss,'YYYY') AS year
                FROM SGDO.DONNEES_CTD sg, SGDO.JEU_DONNEES jd, SGDO.MISSION_EXP mi
                WHERE
                jd.seq_jd = sg.seq_jd
                AND mi.acr_miss=jd.acr_miss
                AND(sg.LATD BETWEEN ", start_lat ," AND ",end_lat," AND sg.LOND BETWEEN ", start_lon ," AND ", end_lon ,")
                AND TO_CHAR(sg.sytm, 'YYYY/MM/DD HH24:MI:SS') >= '",start_date,"'
                AND TO_CHAR(sg.sytm, 'YYYY/MM/DD HH24:MI:SS') <= '",end_date, "'
                AND sg.PSAL IS NOT NULL
                AND sg.TE90 IS NOT NULL")

    # NUMTODSINTERVAL(TO_DATE(sg.sytm, 'YYYY/MM/DD HH24:MI:SS') - TO_DATE ('",sample_date,"' , 'YYYY/MM/DD HH24:MI:SS'), 'SECOND') as daysdifference,
    #remove from query, caused problems
  rs <- ROracle::dbSendQuery(conn,sql_ctd)
  results<-ROracle::fetch(rs)

  if(!is.null(station) & nrow(results >1)){
  if(nrow(results)>1 & station[i] %in% results$STATION_CTD) results<-  results %>% dplyr::filter(STATION_CTD==station[i])
  ctd_JD=results$SEQ_JD
}

  if(is.null(station) | nrow(results)>1){

    ctd_sf <- sf::st_as_sf(results, coords = c("LOND", "LATD"))
    sf::st_crs(ctd_sf) <- 4326

    dat_sf <- sf::st_as_sf(as.data.frame(cbind(latitude=latitude[i],longitude= longitude[i])), coords = c("longitude", "latitude"))
    sf::st_crs(dat_sf) <- 4326
    #cal_sf<-st_transform(cal_sf, crs=lcc)

    #dustance is in meters does not work with lat long 0.2
   ctd_JD= suppressMessages(sf::st_join(dat_sf, ctd_sf, join = nngeo::st_nn)$SEQ_JD)
   results <- results %>%  dplyr::filter(SEQ_JD==ctd_JD)

  }

  if(nrow(results ==1)){
    ctd_JD=results$SEQ_JD
  }

  if(nrow(results !=0)){



###ctd###
  sql_ctd2<-paste0("SELECT
                sg.deph,
                sg.te90,
                sg.psal,
                sg.dens,
                sg.doxy,
                sg.flor,
                sg.cdom,
                sg.psar,
                sg.spar,
                sg.pht_,
                sg.trb_,
                jd.PROFD_MAX_JD
                FROM SGDO.DONNEES_CTD sg,
                SGDO.JEU_DONNEES jd
                WHERE
               sg.SEQ_JD = jd.SEQ_JD
                AND sg.seq_jd IN('",ctd_JD,"')")
  rs <- ROracle::dbSendQuery(conn,sql_ctd2)
  results2<-ROracle::fetch(rs)
  results2$ID <- ID[i]
  }


  if(nrow(results) ==0) results2 <-  data.frame(ID=ID[i])

  #########Bottles#########
  sql_bot<-paste0("SELECT DISTINCT
                sg.seq_jd,
                sg.latd,
                sg.lond,
                sg.sytm,
                jd.COT_QUAL_JD,
                jd.PROFD_MAX_JD,
                jd.stat_jd AS Station_bot,
                ABS(sg.latd -", sample_lat ,") AS latdiff,
                ABS(",sample_lon ," - sg.lond) AS longdiff,
                mi.desc_miss,
                mi.acr_miss,
                TO_CHAR(mi.dat_deb_miss,'YYYY') AS year
                FROM SGDO.DONNEES_BOUTEILLES sg, SGDO.JEU_DONNEES jd, SGDO.MISSION_EXP mi
                WHERE
                jd.seq_jd = sg.seq_jd
                AND mi.acr_miss=jd.acr_miss
                AND(sg.LATD BETWEEN ", start_lat ," AND ",end_lat," AND sg.LOND BETWEEN ", start_lon ," AND ", end_lon ,")
                AND TO_CHAR(sg.sytm, 'YYYY/MM/DD HH24:MI:SS') >= '",start_date,"'
                AND TO_CHAR(sg.sytm, 'YYYY/MM/DD HH24:MI:SS') <= '",end_date, "'
                AND sg.cphl IS NOT NULL")

  rs <- ROracle::dbSendQuery(conn,sql_bot)
botresults<-ROracle::fetch(rs)

  if(!is.null(station) & nrow(botresults >1)){
    if(nrow(botresults)>1 & station[i] %in% botresults$STATION_BOT) botresults<-  botresults %>% dplyr::filter(STATION_BOT==station[i])
    bot_JD=botresults$SEQ_JD
  }

  if(is.null(station) | nrow(botresults)>1){

    bot_sf <- sf::st_as_sf(botresults, coords = c("LOND", "LATD"))
    sf::st_crs(bot_sf) <- 4326

    dat_sf <- sf::st_as_sf(as.data.frame(cbind(latitude=latitude[i],longitude= longitude[i])), coords = c("longitude", "latitude"))
    st_crs(dat_sf) <- 4326
    #cal_sf<-st_transform(cal_sf, crs=lcc)

    #dustance is in meters does not work with lat long 0.2
    bot_JD= suppressMessages(sf::st_join(dat_sf, bot_sf, join = nngeo::st_nn)$SEQ_JD)
    botresults <- botresults %>%  dplyr::filter(SEQ_JD==bot_JD)

  }

  if(nrow(botresults ==1)){
    bot_JD=botresults$SEQ_JD
  }
if(nrow(botresults !=0)){
  ###bot###
  sql_bot2<-paste0("SELECT
                sg.deph,
                sg.cphl,
                jd.PROFD_MAX_JD
                FROM SGDO.DONNEES_BOUTEILLES sg,
                SGDO.JEU_DONNEES jd
                WHERE
               sg.SEQ_JD = jd.SEQ_JD
                AND sg.seq_jd IN('",bot_JD,"')")
  rs <- ROracle::dbSendQuery(conn,sql_bot2)
  botresults2 <- ROracle::fetch(rs)
  botresults2$ID <- ID[i]

}
if(nrow(botresults) ==0) botresults2 <-  data.frame(ID=ID[i], CPHL=NA)

prof<- suppressMessages(list(xbresults2, results2, botresults2) %>%  reduce(dplyr::full_join))
stop(View(prof))

if(!is.null(depth.max)) prof$depth.max = depth.max[i]
if(is.null(depth.max)) prof$depth.max =max(prof$PROFD_MAX_JD)

if(ncol(results2) >3 | ncol(xbresults2)>3){
ctd<- summarize_CTD(data=prof,depth_range=range_ctd)
}
if(ncol(botresults2) >3){
bot <-  summarize_BOT_STRAT(data=prof, depth_range=range_bot)
}
if(!ncol(botresults2) >3){
  bot <- data.frame(ID=ID[i])
}

if(ncol(prof)>3){
ctd_id <-  dplyr::bind_rows(ctd_id, suppressMessages(dplyr::full_join(ctd, bot)))
}

ROracle::dbDisconnect(conn)
}
return(ctd_id)
}

#' Summarizing CTD profiles
#'
#' @param data CTD data
#' @param depth_range vector of 2 values identifying the range of temperature and salinity for average
#'
#' @return a data frame of 1 row for all variables calculated for the unique identifier
#' @export
#'

summarize_CTD<- function(data,depth_range=c(0,50)){
  col_out<-  c(paste0("T", paste(depth_range, collapse="_")),paste0("S", paste(depth_range, collapse="_")),"T_NB","S_NB","T_SURF","S_SURF", "STRAT","Tmin")



data <-  data %>%  dplyr::arrange(DEPH) %>%  dplyr::filter(is.na(CPHL))

    nominal_depth<-ifelse(round(max(data$depth.max)*0.85,0) > max(data$DEPH),max(data$DEPH),round(max(data$depth.max)*0.85,0))

    Tmin<-min(data[,"TE90"], na.rm=T)

    T_NB<-data[which(round(data$DEPH,0)==round(nominal_depth,0))[1],"TE90"]
    S_NB<-data[which(round(data$DEPH,0)==round(nominal_depth,0))[1],"PSAL"]

    surf_depth <- ifelse(5 %in% data$DEPH, 5, min(data$DEPH))

    T_SURF<-data[which(data$DEPH == surf_depth),"TE90"]
    DENS_SURF<-data[which(data$DEPH == surf_depth),"DENS"]
    S_SURF<-data[which(data$DEPH == surf_depth),"PSAL"]

    depth50 <- ifelse(50 %in% data$DEPH, 50,
                      ifelse(max(data$DEPH) <50, max(max(data$DEPH)),
                             ifelse(49 %in% data$DEPH, 49,
                                    ifelse(51 %in% data$DEPH, 51,NA
                                    ))))
    DENS_50<-data[which(data$DEPH == depth50),"DENS"]

    STRAT<-DENS_50-DENS_SURF
    #which row fit the depth range
    row_range<-which(data$DEPH>=depth_range[1] & data$DEPH<=depth_range[2])

    #calculate averages
    #temp<-cbind(u,mean(data[row_range,"Temp"], na.rm=T),mean(data[row_range,"Salinity"], na.rm=T),mean(data[row_range,"DENS"], na.rm=T),STRAT,T_NB,S_NB,T_SURF,S_SURF)
    temp<-as.data.frame(cbind(mean(data[row_range,"TE90"], na.rm=T),
                              tryCatch({mean(data[row_range,"PSAL"], na.rm=T)}, error = function(e) {return(NA)}),
                              T_NB,
                              ifelse(is.null(S_NB), NA, S_NB),
                              T_SURF,
                              ifelse(is.null(S_SURF), NA, S_SURF),
                              ifelse(is.null(STRAT), NA, STRAT),
                              Tmin))

    colnames(temp)<-col_out
    temp$ID <-  data[1,"ID"]

  return(temp)
  }


####bouteilles

#' Chlorophyll trapezoidal integration
#'
#' @param data bottles data at discrete depth
#' @param depth_range vector of 2 values, for chlrophyll integration
#'
#' @return a data frame of 1 row for all variables calculated for the unique identifier
#' @export

summarize_BOT_STRAT<- function(data, depth_range=c(0,100)){

  data <-  data %>%  dplyr::arrange(DEPH) %>% dplyr::filter(!is.na(CPHL)) %>%  dplyr::select(ID,DEPH, depth.max, CPHL)


    integration<-DIS_Profile_Integration(df.data=data, depth_range=depth_range)
    integration<- tidyr::pivot_wider(integration, 1, names_from="variable", values_from = "value")

    fil<- dplyr::filter(data, DEPH < 5)

    if(nrow(fil)> 0) integration$CPHLSURF <-  data[which.min(fil$DEPH),"CPHL"]
    if(nrow(fil)== 0) integration$CPHLSURF <-  NA


    return(integration)
}



#' 1st function for cholorphyll integration
#'
#' @param df.data bottle data
#' @param depth_range vector of 2 values
#'
#' @return helper function
#' @export
#'

DIS_Profile_Integration <- function(df.data, depth_range) {
  var_name <- base::setdiff(names(df.data), c("ID", "DEPH", "depth.max"))

  df <- data.frame()
  for (i_var in var_name) {
    df <- rbind(df,
                df.data %>%
                  dplyr::select("ID", "DEPH", i_var, "depth.max") %>%
                  dplyr::rename("value"=i_var) %>%
                  dplyr::group_by(ID) %>%
                  dplyr::summarize(value=DIS_Integrate_Profile(DEPH, value, depth.max, depth_range)) %>%
                  dplyr::mutate(var_name=i_var, z1=depth_range[1], z2=depth_range[2]) %>%
                  tidyr::unite(variable, var_name, z1, z2, sep="_"))
  }

  # output
  return(df)
}


#' 2nd function for cholorphyll integration
#'
#' @param DEPH helper function
#' @param value helper function
#' @param depth.max helper function
#' @param depth_range helper function

#'
#' @return helper functions
#' @export
#'

DIS_Integrate_Profile <- function(DEPH, value, depth.max, depth_range) {

    # order depth_range
  depth_range <- c(min(depth_range), max(depth_range))

  # check upper intergration limit vs nominal depth
  if (depth_range[1] > depth.max[1]) {
    return(NA)
  }
  # check lower intergration limit vs nominal depth
  if (depth_range[2] > depth.max[1]) {
    depth_range[2] <- depth.max[1]
  }
  rm(depth.max)

  # remove NaN from data
  na_index <- is.na(value)
  if (any(na_index)) {
    DEPH <- DEPH[!na_index]
    value <- value[!na_index]
  }

  if (length(value)<=1) {
    return(NA)
  } else {
    # extrapolate profile
    # at the top
    index <- which.min(DEPH)
    min_depth <- DEPH[index]
    if (depth_range[2] < min_depth) {
      return(NA)
    } else  if (depth_range[1] < min_depth) {
      DEPH <- c(DEPH, depth_range[1])
      value <- c(value, value[index])
    }
    # at the bottom
    index <- which.max(DEPH)
    max_depth <- DEPH[index]
    if (depth_range[1] > max_depth) {
      return(NA)
    } else if (depth_range[2] > max_depth) {
      DEPH <- c(DEPH, depth_range[2])
      value <- c(value, value[index])
    }

    # interpolate profile
    index <- !(depth_range %in% DEPH)
    if (any(index)) {
      tmp_depth <- depth_range[index]
      tmp_value <- pracma::interp1(DEPH, value, tmp_depth, method="linear")
      DEPH <- c(DEPH, tmp_depth)
      value <- c(value, tmp_value)
    }

    # sort the profile
    index <- order(DEPH)
    DEPH <- DEPH[index]
    value <- value[index]

    # calculate integration
    # calculate integration
    index <- DEPH>=depth_range[1] & DEPH<=depth_range[2]


    return(Trapz(DEPH[index],value[index]))


  }
}

#' 3rd function for cholorphyll integration
#'
#' @param x helper function
#' @param y helper function
#'
#' @return helper function
#' @export
#'

Trapz <- function(x, y) {

  # Integral of Y with respect to X using the trapezoidal rule.
  #
  # Input: x: Sorted vector of x-axis values - numeric vector
  #        y:	Vector of y-axis values - numeric vector
  #
  # Output: int : Integral of Y with respect to X - numeric scalar
  #
  # Last update: 20141101
  # Benoit.Casault@dfo-mpo.gc.ca

  idx = 2:length(x)
  return(as.double( (x[idx] - x[idx-1]) %*% (y[idx] + y[idx-1])) / 2)
}





