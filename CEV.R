rm(list=ls())
library(haven)
library(dplyr)
library(stringr)
library(purrr) 
library(tidyr) 
library(readr) 

#new comment again jfjjfff hjgdshjdgsjkhsdgjhkgsd
# Load special libraries (that are not centrally handled)
path <- "shareddata/packages"
library(BTYD,lib=path)
library(tictoc,lib=path) 

# Load anna's rewritten versions of the ML functions
#source("shareddata/anna/programs/cev/btyd_pareto.R")  # Pareto model fix for BTYD package
#source("shareddata/anna/programs/cev/btyd_bgnbd.R")  # BGNBD model fix for BTYD package

#############################
#--------- PARAMS ----------#
#############################

lastObs <- "20181031"
sampFrac <- 1
predYears <- 1
spend_par_start = c(1, 1, 1)
order_par_start = c(1, 3, 1, 3)

period = substr(lastObs,1,6)
lastObsDate <- as.Date(lastObs,"%Y%m%d")

####################################
### Set up folders and filenames ###
####################################

dir.create(file.path("shareddata/anna/data/cev/output", lastObs),
           showWarnings = FALSE)

dir.create(file.path("shareddata/anna/data/cev/input", lastObs),
           showWarnings = FALSE)

outputPath <-
  paste("shareddata/anna/data/cev/output", lastObs, sep = "/")

cbsPath <-
  paste("shareddata/anna/data/cev/input", lastObs,  sep = "/") 

# copy from cloudstorage to indata folder
infiles <- list.files("cloudstorage",paste("^cev_cbs_.+",lastObs,"[.]sas7bdat$",sep=""),full.names=TRUE)
file.copy(infiles, cbsPath,overwrite=TRUE)
file.remove(infiles)

cbsFiles <-
  list.files(cbsPath, full.names = TRUE, ignore.case = TRUE) %>%
  as.list()

outputFiles <-
  list.files(cbsPath, full.names = FALSE, ignore.case = TRUE) %>%
  str_split(., "\\.", simplify = TRUE) %>% 
  .[, 1] %>%
  str_replace(., "_cbs", "") %>% 
  as.list()


##################################
#-------- CEV Function ----------#
##################################

getCEVbg <- function(file,
                     sampFrac,
                     spend.par.start,
                     order.par.start,
                     outputFile) {
  tic()
  
  cev <- read_sas(data_file = file) %>%
    dplyr::rename(
      cust = customer_id,
      t.x = t_x,
      T.cal = T_cal,
      m.x = m_x
    ) %>%
    dplyr::group_by(lastObsDate, country_id, version) %>%
    nest() %>%
    dplyr::mutate(
      sample = data %>% map( ~ dplyr::sample_frac(.x, sampFrac))
      ,
      aovParams = sample %>% map(
        ~ spend.EstimateParameters(
          m.x.vector = .x$m.x,
          x.vector = .x$x,
          par.start = spend.par.start
        )
      )
      ,
      cbs = sample %>% map( ~ as.matrix(dplyr::select(., x, t.x, T.cal)))
      ,
      orderParams = cbs %>% map( ~ bgnbd.EstimateParameters(., par.start = order.par.start))
    ) 
  
  
  print(paste(outputPath, "/", outputFile, "_params_sessionDuration.csv", sep = ""))
  
  cev %>% unnest(aovParams) %>%
    dplyr::mutate(order = row_number(),param="sessionDuration") %>% dplyr::rename(value=aovParams) %>% 
    write_csv(.,
              path = paste(outputPath, "/", outputFile, "_params_sessionDuration.csv", sep = ""))
  print(paste(outputPath, "/", outputFile, "_params_sessionCount.csv", sep = ""))
  cev %>% unnest(orderParams) %>%
    dplyr::mutate(order = row_number(),param="sessionCount") %>% dplyr::rename(value=orderParams) %>% 
    write_csv(.,
              path = paste(outputPath, "/", outputFile, "_params_sessionCount.csv", sep = ""))
  
  rm(cev) 
  
  toc()
  
} 


#######################################
#-------- Call CEV Function ----------#
#######################################

walk2(
  .x = cbsFiles,
  .y = outputFiles,
  ~ getCEVbg(
    file = .x,
    sampFrac = sampFrac,
    spend.par.start = spend_par_start,
    order.par.start = order_par_start,
    outputFile = .y
  )
)

######################################################
### Copy output files to cloudstorage for transfer ###
######################################################

# List all files in the output folder that starts with 'cev_' and ends with '.csv' (regex)
outfiles <- list.files(outputPath,"^cev_.+[.]csv$",full.names=TRUE)

# Copy the files into a joint folder in 'cloudstorage' for transfer readiness
dir.create(file.path("cloudstorage","cev_param"), showWarnings = FALSE)
dir.create(file.path("cloudstorage/cev_param",lastObs), showWarnings = FALSE)
file.copy(outfiles,paste("cloudstorage/cev_param",lastObs,sep="/"),overwrite=TRUE)

# Copy the files one by one to 'cloudstorage' for transfer readiness
#file.copy(outfiles, "cloudstorage",overwrite=TRUE)



