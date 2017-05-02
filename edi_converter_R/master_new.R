#rm(list=ls())
setwd("Q:/Business Intelligence/Projects/BIR/BIR-000493 Clearinghouse QA/Bryan/files")
###################################################################################
###################################################################################
#load needed libraries
library(sqldf)      #need for sqldf function
library(plyr)       #need for join function
library(zoo)        #need for na.locf function
library(stringr)    #need for str_count and str_split_fixed
library(dplyr)      #need for do, count, rename functions
library(data.table) #need to convert data frame to data table
library(tidyr)      #need for separate
###################################################################################
###################################################################################
#Default rows & column counts
#Edit the CSV directly if/when this needs updating
my_defaults <- read.table("defaults.csv",sep=",", header=TRUE)


#What are all the files in the folder
my_files <- list.files(pattern=".txt")
edi_final_master <- NULL
edi_errors_master <- NULL

#loop through all files
for (f in my_files) {

#Import Data - Fill in Blanks, Skip First 2 rows, new line on carriage return, single quotes addressed
#edi_orig <- read.table("szrnslc_17342219.txt", header=FALSE, fill=TRUE, skip=2, sep="\r", quote=NULL)
#edi_orig <- read.table("bryan2.txt", header=FALSE, fill=TRUE, skip=2, sep="\r", quote=NULL)
  edi_orig <- read.table(f,header=FALSE,fill=TRUE,skip=2,sep="\r",quote=NULL)
  #Cut off final 2 rows (they are a file footer)
  edi_orig <- head(edi_orig,-2)
  
  
  #extract VPDI code
  edi_vpdi <- read.table(f,header=FALSE,nrows=1)
 
      #hard-coding the substring works for all schools, may have to re-visit this and conver to a dynamic approach some day
      #due to time constraints on this project, I am leaving it as-is for now
      vpdi_code <- substr(edi_vpdi$V3,8,15)
      if (vpdi_code=="00134600") {vpdi<-"ACC"}
      if (vpdi_code=="02276900")  {vpdi<-"CCA"}
      if (vpdi_code=="00954200")  {vpdi<-"CCD"}
      if (vpdi_code=="00135900")  {vpdi<-"CNCC"}
      if (vpdi_code=="00793300")  {vpdi<-"FRCC"}
      if (vpdi_code=="00135500")  {vpdi<-"LCC"}
      if (vpdi_code=="00998100")  {vpdi<-"MCC"}
      if (vpdi_code=="00136100")  {vpdi<-"NJC"}
      if (vpdi_code=="00136200")  {vpdi<-"OJC"}
      if (vpdi_code=="02116300")  {vpdi<-"PCC"}
      if (vpdi_code=="00889600")  {vpdi<-"PPCC"}
      if (vpdi_code=="00954300")  {vpdi<-"RRCC"}
      if (vpdi_code=="00136800")  {vpdi<-"TSJC"}
   
  #add a pipe to the end of each line in the flat file so we can concat into one line per record
  edi_orig$V1 <- paste(edi_orig$V1,'|',sep='')
###################################################################################
###################################################################################
#make separate data frame with starting point for each record                #sqldf
edi_start <- sqldf("select * from edi_orig where substr(V1,1,3) like '%ST|'")
  edi_start$record_numb <- rownames(edi_start)
###################################################################################
###################################################################################
#merge original & start to our working data set                               #plyr
edi_work <- join(edi_orig,edi_start)
  #down-fill the NA for record numbers for each record                         #zoo
  edi_work <- transform(edi_work, record_numb = as.integer(na.locf(record_numb)))
  #find location of first pipe for each row - escape pipe                  #stringr
  edi_work$firstpipeloc <- str_locate(pattern='\\|',edi_work$V1)
  #new field of all the characters up to the first pipe
  edi_work$uptopipe <- substr(edi_work$V1, 1, edi_work$firstpipeloc-1)
###################################################################################
###################################################################################
#Count occurrances of u2p per record
u2ppr <- count(edi_work, record_numb, uptopipe) %>% rename(u2p_per_rec=n)      #dplyr
  max_u2ppr <- u2ppr[,c(2:3)]
  max_u2ppr <- max_u2ppr %>% group_by(uptopipe) %>% filter(row_number(u2p_per_rec)==n())
  #default maxcount for each uptopipe
  temp_defaults <- my_defaults[,c(1,3)]
  max_u2ppr <- join(max_u2ppr, temp_defaults) %>% rename(maxcount=maxcount_default) #dplyr
###################################################################################
###################################################################################  
#Count number of pipe per record for creation of dummy rows
cp <- edi_work[,c("V1","uptopipe")]
  cp$countpipe <- str_count(cp$V1,"\\|")
  cp <- cp[,c(2:3)]
  #count the max pipes for each uptopipe
  cp2 <- cp %>% group_by(uptopipe) %>% filter(row_number(countpipe)==n())
  #overall_max_cp <- max(cp2$countpipe) #every single row should have this many pipes
  overall_max_cp <- max(my_defaults$maxpipe_default)
###################################################################################
###################################################################################   
#Stats for U2P values
  stats <- join(cp2, max_u2ppr) %>% rename(maxpipe=countpipe) #dplyr  
    stats$overall_max_cp <- overall_max_cp
  #convert to table so pipes field is not a data frame once inserted
  setDT(stats)
  #add in number of pipes for a blank line
  #using 20 as the second number of the substr that is the max in our my_default file for now
  stats$V1 <- sqldf("select uptopipe||substr('||||||||||||||||||||||||||||',1,overall_max_cp)  from stats")
  #need to sort in custom order   --- HARD CODED HARD CODED HARD CODED HARD CODED
  sort_stats <- c("ST","BGN","ENR","DTP","SUM","N1","ENT","IN2","DMG",
                  "N3","N4","SES","FOS","NTE","SE")
      stats <- stats[match(sort_stats,stats$uptopipe)]  #sort the way we need 
      rm(sort_stats)   #remove sort_stats variable, no longer needed
  #Build out standardized order for each u2p
  stats$u2p_order <- as.integer(rownames(stats))
###################################################################################
###################################################################################    
#Need to ensure each record has each type of u2P, so build a "holder" table
rtm <- sqldf("select distinct sp.record_numb, s.uptopipe, s.V1, s.u2p_order,
                     s.maxcount, s.maxpipe
               from edi_start sp,
               stats s")
  rtm$record_numb <- as.integer(rtm$record_numb)
###################################################################################
###################################################################################     
#Pipe Padding
#insert u2p rows for records where that u2p does not exist
edi_work  <- sqldf("select ifnull(df.V1, s.V1) as V1, 
                   ifnull(df.record_numb,s.record_numb) as record_numb, 
                   ifnull(df.uptopipe,s.uptopipe) as uptopipe, 
                   s.u2p_order,
                   s.maxpipe,
                   s.maxcount
                   from rtm s
                   left outer join edi_work df on df.record_numb = s.record_numb and df.uptopipe = s.uptopipe
                   order by ifnull(df.record_numb,s.record_numb), s.u2p_order")
#pipes in each row
edi_work$countpipe <- str_count(edi_work$V1,"\\|")
edi_work$overall_max_pipe <- overall_max_cp
#ALL V1 should have same # of pipes (regardless of uptopipe value)
    
  #perform padding: new work data frame for those records which need padding
  hold_2_pad <- subset(edi_work, countpipe < overall_max_pipe)
    #Perform padding  
    #recalculate countpipe
    hold_2_pad$countpipe <- str_count(hold_2_pad$V1,"\\|")  
    hold_2_pad$pipes_to_add <- hold_2_pad$overall_max_pipe-hold_2_pad$countpipe
    hold_2_pad$pipeholder <- as.character('||||||||||||||||||||||||||||||||||||||||||||||||||||||')
    hold_2_pad$V1 <- as.character(paste(hold_2_pad$V1, substr(hold_2_pad$pipeholder,1,hold_2_pad$pipes_to_add),sep=''))
    hold_2_pad <- hold_2_pad[,c(1:8)]
  #new hold data frame for those records not needing padding
  hold_2_hold <- subset(edi_work, countpipe >= overall_max_pipe)
  #join padded and non-padded data back together
  edi_work <-   rbind(hold_2_pad, hold_2_hold)
     rm(hold_2_hold,hold_2_pad) #no longer needed
  edi_work <- edi_work[with(edi_work,order(record_numb,u2p_order)),] #reorder
  
  #check out countpipes - recalculate
  edi_work$countpipe <- str_count(edi_work$V1,"\\|")
###################################################################################
###################################################################################  
#Dummy Row Creation & Insertion
  #count each unique u2p record type per record so we know how many rows we will need to insert  
  hold_count_rpr <- count(edi_work, record_numb, uptopipe) %>% rename(u2p_per_rec=n)
  edi_work <- merge(edi_work, hold_count_rpr, by=c("record_numb","uptopipe"))
      rm(hold_count_rpr) #no longer need this
      edi_work <- edi_work[with(edi_work,order(record_numb,u2p_order)),] #reorder  
  #how many rows are missing per record per u2p
  edi_work$empty_rows_needed <- edi_work$maxcount - edi_work$u2p_per_rec
  edi_work$whichfile <- f
  #edi_work$whichfile <- "szrnslc_17342219.txt"
  
  #if mybreak is a positive number, cool, go on
  #if mybreak is negative, at least 1 record in that file has errors because there are more values for an u2p than allowed in my_defaults
    #will loop to the next file
    #will write the error records to a new file
  mybreak <- min(edi_work$empty_rows_needed)
  if (mybreak < 0) {print("Processing Error: too many rows for a certain uptopipe. Errors in these records: subset(edi_work[,c(1,2,10,11)], empty_rows_needed < 0)");
    edi_work$vpdi <- vpdi;
    edi_errors_master <- rbind(edi_errors_master,edi_work);
    next}
  
  
  #Creating blank rows
  rows_to_make <- edi_work[,c("record_numb","uptopipe","empty_rows_needed","u2p_order")]
    rows_to_make <- subset(rows_to_make,empty_rows_needed > 0)
    rows_to_make <- sqldf("select distinct * from rows_to_make")
    rows_to_make <- join(rows_to_make, stats)
    rows_to_make <- rows_to_make[,c("record_numb", "uptopipe", "V1", "empty_rows_needed","u2p_order")]
    rows_to_make$row_numb <- rownames(rows_to_make)
    #loop to insert dummy data
    my_new_rows <- rows_to_make[rep(1:nrow(rows_to_make), rows_to_make[,4]),][c(1,2,3,5)] 
    row.names(my_new_rows) <- NULL
    rm(rows_to_make) #no longer need
###################################################################################
################################################################################### 
#Merge everything together
my_old_rows <- edi_work[,c("record_numb","uptopipe","V1","u2p_order")]
edi_final <- rbind(my_old_rows, my_new_rows)
  edi_final <- edi_final[with(edi_final,order(record_numb,u2p_order)),] #reorder
###################################################################################
################################################################################### 
###################################################################################
################################################################################### 
parse_edi <- sqldf("select * from edi_final")
  #need a sequence for the U2P within each record
  parse_edi$u2p_within_rec <- ave(parse_edi$V1,by=parse_edi$uptopipe,parse_edi$record_numb,FUN=seq_along)
parse_edi_st  <- sqldf("select * from parse_edi where uptopipe IN ('ST')") 
parse_edi_bgn <- sqldf("select * from parse_edi where uptopipe IN ('BGN')")
parse_edi_enr <- sqldf("select * from parse_edi where uptopipe IN ('ENR')")
parse_edi_dtp <- sqldf("select * from parse_edi where uptopipe IN ('DTP')")
parse_edi_sum <- sqldf("select * from parse_edi where uptopipe IN ('SUM')")
parse_edi_n1  <- sqldf("select * from parse_edi where uptopipe IN ('N1')")
parse_edi_ent <- sqldf("select * from parse_edi where uptopipe IN ('ENT')")
parse_edi_in2 <- sqldf("select * from parse_edi where uptopipe IN ('IN2')")
parse_edi_dmg <- sqldf("select * from parse_edi where uptopipe IN ('DMG')")
parse_edi_n3  <- sqldf("select * from parse_edi where uptopipe IN ('N3')")
parse_edi_n4  <- sqldf("select * from parse_edi where uptopipe IN ('N4')")
parse_edi_ses <- sqldf("select * from parse_edi where uptopipe IN ('SES')")
parse_edi_fos <- sqldf("select * from parse_edi where uptopipe IN ('FOS')")
parse_edi_nte <- sqldf("select * from parse_edi where uptopipe IN ('NTE')")
parse_edi_se  <- sqldf("select * from parse_edi where uptopipe IN ('SE')")
  #need to parse through them once at a time so each one will have different column names
#1 - ST - 5 pipe max - 1 occurrance max (no loop)
parse_edi_st  <- parse_edi_st %>% separate(V1, c("ST_HEADER","ST_TS_CODE","ST_TS_ID","ST_D","ST_E","ST_F","ST_G","ST_H","ST_I","ST_J","ST_K","ST_L","ST_M","ST_N","ST_O","ST_P","ST_Q","ST_R","ST_S","ST_T","ST_U"),"\\|")
parse_edi_st <- parse_edi_st[,c(1,2:subset(my_defaults,uptopipe=='ST')$maxpipe_default + 1)]

#2 - BGN - 9 pipe max - 1 occurrance max (no loop)      
parse_edi_bgn <- parse_edi_bgn %>% separate(V1, c("BGN_HEADER","BGN_TS_PURPOSE","BGN_TS_ID","BGN_CCCS_TRANS_DATE","BGN_CCCS_TRANS_TIME","BGN_TIME_CODE","BGN_G","BGN_H","BGN_I","BGN_J","BGN_K","BGN_L","BGN_M","BGN_N","BGN_O","BGN_P","BGN_Q","BGN_R","BGN_S","BGN_T","BGN_U"),"\\|")
parse_edi_bgn <- parse_edi_bgn[,c(1,2:subset(my_defaults,uptopipe=='BGN')$maxpipe_default + 2)]

#3 - ENR - 20 pipe max - 10 occurrances max (needs loop)
#break out V1 into columns by the pipe
parse_edi_enr <- parse_edi_enr %>% separate(V1, c("ENR_HEADER","ENR_STATUS_REASON","ENR_LEVEL","ENR_GRAD_DT_FMT","ENR_ANTIC_GRAD_DT","ENT_MAJOR","ENT_MIN_MEAS_RNG","ENT_MAX_MEAS_RNG","ENT_GPA","ENT_COND_A","ENT_COND_B","ENT_COMPULS_TERM","ENT_DATE_REPORTED_FMT","ENT_CERT_DATE","ENT_FOOTER","ENT_P","ENT_Q","ENT_R","ENT_S","ENT_T","ENT_U"),"\\|")
#break out so each occurance within a record_numb gets unique column names
parse_edi_enr <- dcast(setDT(parse_edi_enr),uptopipe + record_numb + u2p_within_rec ~ u2p_within_rec, value.var=c("ENR_HEADER","ENR_STATUS_REASON","ENR_LEVEL","ENR_GRAD_DT_FMT","ENR_ANTIC_GRAD_DT","ENT_MAJOR","ENT_MIN_MEAS_RNG","ENT_MAX_MEAS_RNG","ENT_GPA","ENT_COND_A","ENT_COND_B","ENT_COMPULS_TERM","ENT_DATE_REPORTED_FMT","ENT_CERT_DATE","ENT_FOOTER","ENT_P","ENT_Q","ENT_R","ENT_S","ENT_T","ENT_U"),fill="")
#keep only the columns we need
parse_edi_enr <- parse_edi_enr[,c(2,4:((subset(my_defaults,uptopipe=='ENR')$maxpipe_default)*(subset(my_defaults,uptopipe=='ENR')$maxcount_default)+3)),with=FALSE]
#group all rows for a record into a single row
parse_edi_enr <- parse_edi_enr[,lapply(.SD,paste0,collapse=""),by=.(record_numb)]
#convert back to data frame
parse_edi_enr <- data.frame(parse_edi_enr)  

#4 - DTP - 4 pipe max - 5 occurrances max (needs loop)
#break out V1 into columns by the pipe
parse_edi_dtp <- parse_edi_dtp %>% separate(V1, c("DTP_HEADER","DTP_DATE_TYPE","DTP_DATE_FORMAT","DTP_DATE","DTP_E","DTP_F","DTP_G","DTP_H","DTP_I","DTP_J","DTP_K","DTP_L","DTP_M","DTP_N","DTP_O","DTP_P","DTP_Q","DTP_R","DTP_S","DTP_T","DTP_U"),"\\|")
#break out so each occurance within a record_numb gets unique column names    
parse_edi_dtp <- dcast(setDT(parse_edi_dtp),uptopipe + record_numb + u2p_within_rec ~ u2p_within_rec, value.var=c("DTP_HEADER","DTP_DATE_TYPE","DTP_DATE_FORMAT","DTP_DATE","DTP_E","DTP_F","DTP_G","DTP_H","DTP_I","DTP_J","DTP_K","DTP_L","DTP_M","DTP_N","DTP_O","DTP_P","DTP_Q","DTP_R","DTP_S","DTP_T","DTP_U"),fill="")
#keep only the columns we need
parse_edi_dtp <- parse_edi_dtp[,c(2,4:((subset(my_defaults,uptopipe=='DTP')$maxpipe_default)*(subset(my_defaults,uptopipe=='DTP')$maxcount_default)+3)),with=FALSE]
#group all rows for a record into a single row
parse_edi_dtp <- parse_edi_dtp[,lapply(.SD,paste0,collapse=""),by=.(record_numb)]  
#convert back to data frame
parse_edi_dtp <- data.frame(parse_edi_dtp)  

#5 - SUM - 11 pipe max - 1 occurrance max (no loop)  -- no clue what the data means 
parse_edi_sum <- parse_edi_sum %>% separate(V1, c("SUM_HEADER","SUM_B","SUM_C","SUM_D","SUM_E","SUM_F","SUM_G","SUM_H","SUM_I","SUM_J","SUM_K","SUM_L","SUM_M","SUM_N","SUM_O","SUM_P","SUM_Q","SUM_R","SUM_S","SUM_T","SUM_U"),"\\|")
parse_edi_sum <- parse_edi_sum[,c(1,2:subset(my_defaults,uptopipe=='SUM')$maxpipe_default + 1)]

#6 - N1 - 6 pipe max - 1 occurrance max (no loop)    
parse_edi_n1  <- parse_edi_n1 %>% separate(V1, c("N1_HEADER","N1_IDENTITY","N1_NAME","D","E","F","G","H","I","J","K","L","M","N","O","SUM_P","SUM_Q","SUM_R","SUM_S","SUM_T","SUM_U"),"\\|")
parse_edi_n1 <- parse_edi_n1[,c(1,2:subset(my_defaults,uptopipe=='N1')$maxpipe_default + 1)]

#7 - ENT - 10 pipe max - 6 occurrance max (needs loop) 
#break out V1 into columns by the pipe
parse_edi_ent <- parse_edi_ent %>% separate(V1, c("ENT_HEADER","ENT_ST_SCH","ENT_IDENTIFIER","ENT_CODE_TYPE","ENT_CODE","ENT_TYPE","ENT_PARTY_CODE_TYPE","ENT_PARTY_CODE","ENT_REF_IDENT_QUALIF","ENT_J","ENT_K","ENT_L","ENT_M","ENT_N","ENT_O","ENT_P","ENT_Q","ENT_R","ENT_S","ENT_T","ENT_U"),"\\|")
#break out so each occurance within a record_numb gets unique column names  
parse_edi_ent <- dcast(setDT(parse_edi_ent),uptopipe + record_numb + u2p_within_rec ~ u2p_within_rec, value.var=c("ENT_HEADER","ENT_ST_SCH","ENT_IDENTIFIER","ENT_CODE_TYPE","ENT_CODE","ENT_TYPE","ENT_PARTY_CODE_TYPE","ENT_PARTY_CODE","ENT_REF_IDENT_QUALIF","ENT_J","ENT_K","ENT_L","ENT_M","ENT_N","ENT_O","ENT_P","ENT_Q","ENT_R","ENT_S","ENT_T","ENT_U"),fill="")
#keep only the columns we need
parse_edi_ent <- parse_edi_ent[,c(2,4:((subset(my_defaults,uptopipe=='ENT')$maxpipe_default)*(subset(my_defaults,uptopipe=='ENT')$maxcount_default)+3)),with=FALSE] 
#group all rows for a record into a single row
parse_edi_ent <- parse_edi_ent[,lapply(.SD,paste0,collapse=""),by=.(record_numb)]  
#convert back to data frame
parse_edi_ent <- data.frame(parse_edi_ent)  

#8 - IN2 - 3 pipes max - 5 occurrance max (needs loop)    
#break out V1 into columns by the pipe
parse_edi_in2 <- parse_edi_in2 %>% separate(V1, c("IN2_HEADER","IN2_NAME_CODE","IN2_NAME","IN2_D","IN2_E","IN2_F","IN2_G","IN2_H","IN2_I","IN2_J","IN2_K","IN2_L","IN2_M","IN2_N","IN2_O","IN2_P","IN2_Q","IN2_R","IN2_S","IN2_T","IN2_U"),"\\|")
#break out so each occurance within a record_numb gets unique column names
parse_edi_in2 <- dcast(setDT(parse_edi_in2),uptopipe + record_numb + u2p_within_rec ~ u2p_within_rec, value.var=c("IN2_HEADER","IN2_NAME_CODE","IN2_NAME","IN2_D","IN2_E","IN2_F","IN2_G","IN2_H","IN2_I","IN2_J","IN2_K","IN2_L","IN2_M","IN2_N","IN2_O","IN2_P","IN2_Q","IN2_R","IN2_S","IN2_T","IN2_U"),fill="")
#keep only the columns we need
parse_edi_in2 <- parse_edi_in2[,c(2,4:((subset(my_defaults,uptopipe=='IN2')$maxpipe_default)*(subset(my_defaults,uptopipe=='IN2')$maxcount_default)+1)),with=FALSE]       
#group all rows for a record into a single row
parse_edi_in2 <- parse_edi_in2[,lapply(.SD,paste0,collapse=""),by=.(record_numb)]  
#convert back to data frame
parse_edi_in2 <- data.frame(parse_edi_in2)  

#9 - DMG - 9 pipes max - 1 occurrance max (no loop)        
parse_edi_dmg <- parse_edi_dmg %>% separate(V1, c("DMG_HEADER","DMG_DATE_FORMAT","DMG_BIRTHDAY","DMG_D","DMG_E","DMG_F","DMG_G","DMG_H","DMG_I","DMG_J","DMG_K","DMG_L","DMG_M","DMG_N","DMG_O","DMG_P","DMG_Q","DMG_R","DMG_S","DMG_T","DMG_U"),"\\|")
parse_edi_dmg <- parse_edi_dmg[,c(1,2:subset(my_defaults,uptopipe=='DMG')$maxpipe_default + 3)]

#####fix below to loop
#10 - N3 - 3 pipes max - 2 occurrance max (no loop)
parse_edi_n3  <- parse_edi_n3 %>% separate(V1, c("N3_HEADER","N3_ADDRESS","N3_C","N3_D","N3_E","N3_F","N3_G","N3_H","N3_I","N3_J","N3_K","N3_L","N3_M","N3_N","N3_O","N3_P","N3_Q","N3_R","N3_S","N3_T","N3_U"),"\\|")
#break out so each occurance within a record_numb gets unique column names
parse_edi_n3 <- dcast(setDT(parse_edi_n3),uptopipe + record_numb + u2p_within_rec ~ u2p_within_rec, value.var=c("N3_HEADER","N3_ADDRESS","N3_C","N3_D","N3_E","N3_F","N3_G","N3_H","N3_I","N3_J","N3_K","N3_L","N3_M","N3_N","N3_O","N3_P","N3_Q","N3_R","N3_S","N3_T","N3_U"),fill="")
#keep only the columns we need
parse_edi_n3 <- parse_edi_n3[,c(2,4:((subset(my_defaults,uptopipe=='N3')$maxpipe_default)*(subset(my_defaults,uptopipe=='N3')$maxcount_default)+3)),with=FALSE]  
#group all rows for a record into a single row
parse_edi_n3 <- parse_edi_n3[,lapply(.SD,paste0,collapse=""),by=.(record_numb)] 
#convert back to data frame
parse_edi_n3 <- data.frame(parse_edi_n3) 

#11 - N4 - 7 pipes max - 1 occurrance max (no loop)    
parse_edi_n4  <- parse_edi_n4 %>% separate(V1, c("N4_HEADER","N4_CITY","N4_STATE","N4_POSTAL","N4_COUNTRY","N4_F","N4_G","N4_H","N4_I","N4_J","N4_K","N4_L","N4_M","N4_N","N4_O","N4_P","N4_Q","N4_R","N4_S","N4_T","N4_U"),"\\|")
parse_edi_n4 <- parse_edi_n4[,c(1,2:subset(my_defaults,uptopipe=='N4')$maxpipe_default + 3)]

#12 - SES - 15 pipes max - 10 occurrance max (needs loop) - no data description
#break out V1 into columns by the pipe
parse_edi_ses <- parse_edi_ses %>% separate(V1, c("SES_HEADER","SES_B","SES_C","SES_D","SES_E","SES_F","SES_G","SES_H","SES_I","SES_J","SES_K","SES_L","SES_M","SES_N","SES_O","SES_P","SES_Q","SES_R","SES_S","SES_T","SES_U"),"\\|")
#break out so each occurance within a record_numb gets unique column names
parse_edi_ses <- dcast(setDT(parse_edi_ses),uptopipe + record_numb + u2p_within_rec ~ u2p_within_rec, value.var=c("SES_HEADER","SES_B","SES_C","SES_D","SES_E","SES_F","SES_G","SES_H","SES_I","SES_J","SES_K","SES_L","SES_M","SES_N","SES_O","SES_P","SES_Q","SES_R","SES_S","SES_T","SES_U"),fill="")
#keep only the columns we need
parse_edi_ses <- parse_edi_ses[,c(2,4:((subset(my_defaults,uptopipe=='SES')$maxpipe_default)*(subset(my_defaults,uptopipe=='SES')$maxcount_default)+3)),with=FALSE]  
#group all rows for a record into a single row
parse_edi_ses <- parse_edi_ses[,lapply(.SD,paste0,collapse=""),by=.(record_numb)]  
#convert back to data frame
parse_edi_ses <- data.frame(parse_edi_ses)  

#13 - FOS - 10 pipes max - 10 occurrance max (needs loop) - no data description 
#break out V1 into columns by the pipe
parse_edi_fos <- parse_edi_fos %>% separate(V1, c("FOS_HEADER","FOS_B","FOS_C","FOS_D","FOS_E","FOS_F","FOS_G","FOS_H","FOS_I","FOS_J","FOS_K","FOS_L","FOS_M","FOS_N","FOS_O","FOS_P","FOS_Q","FOS_R","FOS_S","FOS_T","FOS_U"),"\\|")
#break out so each occurance within a record_numb gets unique column names    
parse_edi_fos <- dcast(setDT(parse_edi_fos),uptopipe + record_numb + u2p_within_rec ~ u2p_within_rec, value.var=c("FOS_HEADER","FOS_B","FOS_C","FOS_D","FOS_E","FOS_F","FOS_G","FOS_H","FOS_I","FOS_J","FOS_K","FOS_L","FOS_M","FOS_N","FOS_O","FOS_P","FOS_Q","FOS_R","FOS_S","FOS_T","FOS_U"),fill="")
#keep only the columns we need
parse_edi_fos <- parse_edi_fos[,c(2,4:((subset(my_defaults,uptopipe=='FOS')$maxpipe_default)*(subset(my_defaults,uptopipe=='FOS')$maxcount_default)+3)),with=FALSE]  
#group all rows for a record into a single row
parse_edi_fos <- parse_edi_fos[,lapply(.SD,paste0,collapse=""),by=.(record_numb)]  
#convert back to data frame
parse_edi_fos <- data.frame(parse_edi_fos)  

# 14 - NTE - 3 pipes max - 10 occurrance max (needs loop) - no data description
#break out V1 into columns by the pipe
parse_edi_nte <- parse_edi_nte %>% separate(V1, c("NTE_HEADER","NTE_B","NTE_C","NTE_D","NTE_E","NTE_F","NTE_G","NTE_H","NTE_I","NTE_J","NTE_K","NTE_L","NTE_M","NTE_N","NTE_O","NTE_P","NTE_Q","NTE_R","NTE_S","NTE_T","NTE_U"),"\\|")  
#break out so each occurance within a record_numb gets unique column names    
parse_edi_nte <- dcast(setDT(parse_edi_nte),uptopipe + record_numb + u2p_within_rec ~ u2p_within_rec, value.var=c("NTE_HEADER","NTE_B","NTE_C","NTE_D","NTE_E","NTE_F","NTE_G","NTE_H","NTE_I","NTE_J","NTE_K","NTE_L","NTE_M","NTE_N","NTE_O","NTE_P","NTE_Q","NTE_R","NTE_S","NTE_T","NTE_U"),fill="")
#keep only the columns we need
parse_edi_nte <- parse_edi_nte[,c(2,4:((subset(my_defaults,uptopipe=='NTE')$maxpipe_default)*(subset(my_defaults,uptopipe=='NTE')$maxcount_default)+3)),with=FALSE]            
#group all rows for a record into a single row
parse_edi_nte <- parse_edi_nte[,lapply(.SD,paste0,collapse=""),by=.(record_numb)]  
#convert back to data frame
parse_edi_nte <- data.frame(parse_edi_nte)  

#15 - SE - 3 pipes max - 1 occurrance max (no loop)
parse_edi_se  <- parse_edi_se %>% separate(V1, c("SE_HEADER","SE_NUM_OF_SEGMENTS_INCLUDED","SE_C","SE_D","SE_E","SE_F","SE_G","SE_H","SE_I","SE_J","SE_K","SE_L","SE_M","SE_N","SE_O","SE_P","SE_Q","SE_R","SE_S","SE_T","SE_U"),"\\|")
parse_edi_se <- parse_edi_se[,c(1,2:subset(my_defaults,uptopipe=='SE')$maxpipe_default + 1)]

      
    
edi_final2 <- join(parse_edi_st, parse_edi_bgn)
edi_final2 <- join(edi_final2,    parse_edi_enr)
edi_final2 <- join(edi_final2,    parse_edi_dtp)
edi_final2 <- join(edi_final2,    parse_edi_sum)
edi_final2 <- join(edi_final2,    parse_edi_n1)
edi_final2 <- join(edi_final2,    parse_edi_ent)
edi_final2 <- join(edi_final2,    parse_edi_in2)
edi_final2 <- join(edi_final2,    parse_edi_dmg)
edi_final2 <- join(edi_final2,    parse_edi_n3)
edi_final2 <- join(edi_final2,    parse_edi_n4)
edi_final2 <- join(edi_final2,    parse_edi_ses)
edi_final2 <- join(edi_final2,    parse_edi_fos)
edi_final2 <- join(edi_final2,    parse_edi_nte)
edi_final2 <- join(edi_final2,    parse_edi_se)

#timestamp the creation  
edi_final2$process_date <- Sys.Date()
#insert VPDI code
edi_final2$mif_value <- vpdi
#which file was it in
edi_final2$whichfile <- f

edi_final_master <- rbind(edi_final2, edi_final_master)
}

#export successful records to to CSV
write.csv(edi_final_master, "edi_final_master.csv", fileEncoding='utf8')

#Timestamp Error File Name so we don't overwrite before we can fix, remove hyphens and colons
currentDate <- Sys.time()
myfilename <- gsub(":","",currentDate)
myfilename <- gsub("-","",myfilename)
myfilename <- paste("edi_row_errors_master",myfilename,".csv",sep="")

write.csv(edi_errors_master, file=myfilename, fileEncoding='utf8')