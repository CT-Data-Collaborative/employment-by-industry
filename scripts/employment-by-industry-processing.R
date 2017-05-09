library(dplyr) 
library(datapkg)
library(readxl)
library(tidyr)

##################################################################
#
# Processing Script for Employment-by-Industry
# Created by Jenna Daly
# On 05/08/2017
#
##################################################################

#Setup environment
sub_folders <- list.files()
data_location <- grep("raw", sub_folders, value=T)
path <- (paste0(getwd(), "/", data_location))
drops <- c("")
round_up = function(x) trunc(x+0.5)

###Create subsets

##Town Data######################################################################################################################

#grabs town xls files
town_path <- file.path(path, "town")
town_xls <- dir(town_path, pattern = "Town")

#Create empty data frame
all_towns <- data.frame(stringsAsFactors = F)
for (i in 1:length(town_xls)) {
  current_file <- (read_excel(paste0(town_path, "/", town_xls[1]), sheet=1, skip=0))
  current_file <- current_file[, !(names(current_file) == "Suppress")]
  current_file <- current_file[, !(names(current_file) == "Total Annual Wages")]
  colnames(current_file) <- c("Town/County", "NAICS Code", "Industry Name", "Number of Employers", 
                              "Annual Average Employment", "Annual Average Wage")
  #remove top row
  current_file <- current_file[-1,]
  #only keep populated columns (remove blank columns)
  current_file <- current_file[ , !(names(current_file) %in% drops)]
  #remove blank rows
  current_file <- current_file[rowSums(is.na(current_file)) != ncol(current_file),]
  #assign year
  get_year <- unique(as.numeric(unlist(gsub("[^0-9]", "", unlist(town_xls[1])), "")))
  get_year <- substr(get_year, 1, 4)
  current_file$Year <- get_year
  #populate blank town rows with corresponding town
  currentTown = current_file[1,1]
  for (i in 1:nrow(current_file)) {
    if(is.na(current_file[i,1])) {
      current_file[i,1] <- currentTown
    } else {
      currentTown <- current_file[i,1]
    }
  }
  current_file <- current_file[current_file$`Town/County`!="Town", ]
  #Configure NAICS code column to filter on
  current_file$`NAICS Code` <- as.numeric(current_file$`NAICS Code`)
  #Set NAICS code to 100 if not assigned (place holder)
  current_file[ , 2][is.na(current_file[ , 2] ) ] = 100 
  #remove all rows that have NAICS code greater than 100
  current_file <- current_file[current_file$`NAICS Code`<=100,]
  #make copy of category column, this new column with assign ownership
  current_file$Ownership <- current_file$`Industry Name`
  #set all values not equal to top level to NA
  top_levels <- c("Total - All Industries", "Total Government", "Federal Government", "State Government", "Local/Municipal Government") 
  current_file <- current_file %>% mutate(Ownership = replace(Ownership, !(Ownership %in% top_levels), NA))
  #populate blank ownership rows with corresponding ownership
  currentownership = current_file[1,8]
  for (i in 1:nrow(current_file)) {
    if(is.na(current_file[i,8])) {
      current_file[i,8] <- currentownership
    } else {
      currentownership <- current_file[i,8]
    }
  }
  #relabel Industry Name column
  current_file$`Industry Name`[current_file$`Industry Name` == "Total - All Industries"] <- "Total Private"
  current_file$`Industry Name`[current_file$`Industry Name` == "Federal Government"] <- "Total Federal Government"
  current_file$`Industry Name`[current_file$`Industry Name` == "State Government"] <- "Total State Government"
  current_file$`Industry Name`[current_file$`Industry Name` == "Local/Municipal Government"] <- "Total Local Government"
  #relabel ownership column
  current_file$Ownership[current_file$Ownership == "Total - All Industries"] <- "Private Ownership"
  current_file$Ownership[current_file$Ownership == "Total Federal Government"] <- "Federal Government"
  current_file$Ownership[current_file$Ownership == "Total State Government"] <- "State Government"
  current_file$Ownership[current_file$Ownership == "Local/Municipal Government"] <- "Local Government"
  #remove columns not needed
  current_file <- current_file[, !(names(current_file) == "NAICS Code")]

  #remove Connecticut rows
  current_file <- current_file[!current_file$`Town/County` == "State of Connecticut",]
  
  #bind together
  all_towns <- rbind(all_towns, current_file) 
} 
  
  
  
  #Convert to long format
  cols_to_stack <- c("Number of Employers", 
                     "Annual Average Employment", 
                     "Annual Average Wage")
  long_row_count = nrow(current_file) * length(cols_to_stack)
  current_file <- reshape(current_file, 
                          varying = cols_to_stack, 
                          v.names = "Value", 
                          timevar = "Variable", 
                          times = cols_to_stack, 
                          new.row.names = 1:long_row_count,
                          direction = "long"
  )
  current_file$id <- NULL
  #Round "Value" column
  #makes sure 0.5 gets rounded as 1.0, transforms "*" to "NA"
  current_file$Value <- round_up(as.numeric(current_file$Value))

#set * to -9999
all_towns[is.na(all_towns)] <- -9999

df[df==""]<-NA

#Merge in FIPS
town_fips_dp_URL <- 'https://raw.githubusercontent.com/CT-Data-Collaborative/ct-town-list/master/datapackage.json'
town_fips_dp <- datapkg_read(path = town_fips_dp_URL)
fips <- (town_fips_dp$data[[1]])

town_long_fips <- merge(all_towns, fips, by.x = "Town/County", by.y = "Town",all=T)

#remove "Connecticut"
town_long_fips <- town_long_fips[ grep("Connecticut", town_long_fips$`Town/County`, invert = TRUE) , ]

#Reorder columns
town_long_fips <- town_long_fips %>% 
  select(`Town/County`, `FIPS`, `Year`, `Ownership`, `Industry Name`, `Variable`, `Value`)

#set NA to -9999
town_long_fips[is.na(town_long_fips)] <- -9999

#Cleanup
rm(all_towns, current_file, currentTown, fips)

##County Data####################################################################################################################

#grabs county xls files
county_path <- file.path(path, "county")
county_xls <- dir(county_path, pattern = "CNTY")


#read in entire xls file (all sheets)
read_excel_allsheets <- function(filename) {
  sheets <- readxl::excel_sheets(filename)
  x <- lapply(sheets, function(X) readxl::read_excel(filename, sheet = X))
  names(x) <- sheets
  x
}

#Create a list of all sheet data for all years
for (i in 1:length(county_xls)) {
  mysheets <- read_excel_allsheets(paste0(county_path, "/", county_xls[i]))
  #Cycle through all sheets, extract sheet name, and assign each sheet (county) its own data frame
  for (j in 1:length(mysheets)) {
    current_county_name <- colnames(mysheets[[j]])[1]
    current_county_file <- mysheets[[j]] 
    get_year <- unique(as.numeric(unlist(gsub("[^0-9]", "", unlist(county_xls[i])), "")))
    get_year <- get_year + 2000
    assign(paste0(current_county_name, "_", get_year), current_county_file)
  }
}

rm(current_county_file)
dfs <- ls()[sapply(mget(ls(), .GlobalEnv), is.data.frame)]
county_data <- grep("County_", dfs, value=T)

all_counties <- data.frame(stringsAsFactors = F)
#Process each county file
for (i in 1:length(county_data)) {
  #grab first county file
  current_county_df <- get(county_data[i])
  current_county_df <- current_county_df[,c(1:7)]
  
  #assign `Town/County` column
  current_county_df$`Town/County` <- colnames(current_county_df)[1]
  #Assign year column
  get_year <- unique(as.numeric(unlist(gsub("[^0-9]", "", unlist(county_data[i])), "")))
  current_county_df$Year <- get_year
  
  #name columns, so they can be selected
  colnames(current_county_df) <- c("Naics Code", "", "Industry Name", "Number of Employers", "Annual Average Employment", 
                                   "Total Annual Wages", "Annual Average Wage", "Town/County", "Year")
  #remove blank rows
  current_county_df <- current_county_df %>% drop_na(`Industry Name`)  
  #remove first row
  current_county_df <- current_county_df[-1,]
  #Configure NAICS code column to filter on
  current_county_df$`Naics Code` <- as.numeric(current_county_df$`Naics Code`)
  #Set NAICS code to 100 if not assigned (place holder)
  current_county_df[ , 1][is.na(current_county_df[ , 1] ) ] = 100 
  #remove all rows that have NAICS code greater than 100
  current_county_df <- current_county_df[current_county_df$`Naics Code`<=100,]
  #make copy of category column, this new column with assign ownership
  current_county_df$Ownership <- current_county_df$`Industry Name`
  #set all values not equal to top level to NA
  top_levels <- c("County Total", "Total Private", "Total Government", "Total Federal Government", "Total State Government", "Total Local Government") 
  current_county_df <- current_county_df %>% mutate(Ownership = replace(Ownership, !(Ownership %in% top_levels), NA))
  #populate blank ownership rows with corresponding ownership
  currentownership = current_county_df[1,10]
  for (i in 1:nrow(current_county_df)) {
    if(is.na(current_county_df[i,10])) {
      current_county_df[i,10] <- currentownership
    } else {
      currentownership <- current_county_df[i,10]
    }
  }
  #relabel ownership column
  current_county_df$Ownership[current_county_df$Ownership == "County Total"] <- "All"
  current_county_df$Ownership[current_county_df$Ownership == "Total Private"] <- "Private Ownership"
  current_county_df$Ownership[current_county_df$Ownership == "Total Federal Government"] <- "Federal Government"
  current_county_df$Ownership[current_county_df$Ownership == "Total State Government"] <- "State Government"
  current_county_df$Ownership[current_county_df$Ownership == "Total Local Government"] <- "Local Government"
  #remove columns not needed
  current_county_df <- current_county_df[, !(names(current_county_df) == "Naics Code")]
  current_county_df <- current_county_df[, !(names(current_county_df) == "")]
  current_county_df <- current_county_df[, !(names(current_county_df) == "Total Annual Wages")]
  current_county_df <- current_county_df[, !(names(current_county_df) == "Average Weekly Wage")]
  
  #bind all counties together
  all_counties <- rbind(all_counties, current_county_df)  
}

#set * to -9999

#set digits
cols <- c("Number of Employers", "Annual Average Employment", "Annual Average Wage")
all_counties[cols] <- sapply(all_counties[cols],as.numeric)

#Convert to long format
long_row_count = nrow(all_counties) * length(cols_to_stack)

county_long <- reshape(all_counties, 
                       varying = cols_to_stack, 
                       v.names = "Value", 
                       timevar = "Variable", 
                       times = cols_to_stack, 
                       new.row.names = 1:long_row_count,
                       direction = "long"
)

county_long$id <- NULL

#set any NAs as missing
all_counties[is.na(all_counties)] <- -6666

#Round "Value" column
#makes sure 0.5 gets rounded as 1.0, transforms "*" to "NA"
county_long$Value <- round_up(as.numeric(county_long$Value))

#Merge in FIPS
county_fips_dp_URL <- 'https://raw.githubusercontent.com/CT-Data-Collaborative/ct-county-list/master/datapackage.json'
county_fips_dp <- datapkg_read(path = county_fips_dp_URL)
fips <- (county_fips_dp$data[[1]])

county_long_fips <- merge(county_long, fips, by.x = "Town/County", by.y = "County", all=T)

#remove "Connecticut"
county_long_fips <- county_long_fips[ grep("Connecticut", county_long_fips$`Town/County`, invert = TRUE) , ]

#Reorder columns
county_long_fips <- county_long_fips %>% 
  select(`Town/County`, `FIPS`, `Year`, `Ownership`, `Industry Name`, `Variable`, `Value`)

#Cleanup
rm(list=ls(pattern="20"), all_counties, county_long, current_county_df, fips)

##State Data#####################################################################################################################

#grabs state xls files
state_path <- file.path(path, "state")
state_xls <- dir(state_path, pattern = "CT")

all_state_years <- data.frame(stringsAsFactors = F)
for (i in 1:length(state_xls)) {
  current_file <- (read_excel(paste0(state_path, "/", state_xls[i]), sheet=1, skip=0))
  current_file <- current_file[,c(1:7)]
  colnames(current_file) <- c("NAICS Code", "Town/County", "Industry Name", "Number of Employers", 
                              "Annual Average Employment", "Total Annual Wages", "Annual Average Wage")
  current_file <- current_file[, !(names(current_file) == "Total Annual Wages")]
  current_file$`Town/County` <- "Connecticut"
  #only keep populated columns (remove blank columns)
  #current_file <- current_file[ , !(names(current_file) %in% drops)]
  current_file <- current_file[-(1:6),]
  #assign year
  get_year <- unique(as.numeric(unlist(gsub("[^0-9]", "", unlist(state_xls[i])), "")))
  get_year <- get_year+2000
  current_file$Year <- get_year
  #remove rows where Industry name is blank
  current_file <- current_file[!is.na(current_file$`Industry Name`),]
  #Configure NAICS code column to filter on
  current_file$`NAICS Code` <- as.numeric(current_file$`NAICS Code`)
  #Set NAICS code to 100 if not assigned (place holder)
  current_file[ , 1][is.na(current_file[ , 1] ) ] = 100 
  #remove all rows that have NAICS code greater than 100
  current_file <- current_file[current_file$`NAICS Code`<=100,]
  #make copy of category column, this new column with assign ownership
  current_file$Ownership <- current_file$`Industry Name`
  #set all values not equal to top level to NA
  top_levels <- c("Statewide Total", "Total Private", "Total Government", "Total Federal Government", "Total State Government", "Total Local Government") 
  current_file <- current_file %>% mutate(Ownership = replace(Ownership, !(Ownership %in% top_levels), NA))
  #populate blank ownership rows with corresponding ownership
  currentownership = current_file[1,8]
  for (i in 1:nrow(current_file)) {
    if(is.na(current_file[i,8])) {
      current_file[i,8] <- currentownership
    } else {
      currentownership <- current_file[i,8]
    }
  }
  #relabel ownership column
  current_file$Ownership[current_file$Ownership == "Statewide Total"] <- "All"
  current_file$Ownership[current_file$Ownership == "Total Private"] <- "Private Ownership"
  current_file$Ownership[current_file$Ownership == "Total Federal Government"] <- "Federal Government"
  current_file$Ownership[current_file$Ownership == "Total State Government"] <- "State Government"
  current_file$Ownership[current_file$Ownership == "Total Local Government"] <- "Local Government"
  #remove columns not needed
  current_file <- current_file[, !(names(current_file) == "NAICS Code")]

  #Convert to long format
  cols_to_stack <- c("Number of Employers", 
                     "Annual Average Employment", 
                     "Annual Average Wage")
  
  long_row_count = nrow(current_file) * length(cols_to_stack)
  
  current_file <- reshape(current_file, 
                          varying = cols_to_stack, 
                          v.names = "Value", 
                          timevar = "Variable", 
                          times = cols_to_stack, 
                          new.row.names = 1:long_row_count,
                          direction = "long"
  )
  current_file$id <- NULL
  #Round "Value" column
  #makes sure 0.5 gets rounded as 1.0, transforms "*" to "NA"
  current_file$Value <- round_up(as.numeric(current_file$Value))
  #bind together
  all_state_years <- rbind(all_state_years, current_file) 
}

#Add FIPS (doing this manually, because only one value)
state_long_fips <- all_state_years
state_long_fips$FIPS <- 9

#Reorder columns
state_long_fips <- state_long_fips %>% 
  select(`Town/County`, `FIPS`, `Year`, `Ownership`, `Industry Name`, `Variable`, `Value`)

#Cleanup
rm(current_file, currentownership, all_state_years)

###Merge to create completed data set
employment_by_industry <- rbind(town_long_fips, county_long_fips, state_long_fips)

#remove duplicates
employment_by_industry <- employment_by_industry[!duplicated(employment_by_industry), ]

#Add Measure Type
employment_by_industry$"Measure Type" <- NA
##fill in 'Measure Type' column based on criteria listed below
employment_by_industry$"Measure Type"[which(employment_by_industry$Variable %in% c("Number of Employers", 
                                                                                   "Annual Average Employment"))] <- "Number"
employment_by_industry$"Measure Type"[which(employment_by_industry$Variable %in% c("Annual Average Wage"))] <- "US Dollars"


employment_by_industry <- arrange(employment_by_industry, `Town/County`, `Year`, `Ownership`, `Variable`)

common <- c("Construction", "Manufacturing", "Retail Trade", "Total Private", "Total Government")

#Define rank
employment_by_industry$Rank <- NA
employment_by_industry$Rank[which(employment_by_industry$`Industry Name` %in% common)] <- -1
#Complete ranking (top 3 in each geography)

if RANK = NA
  for every town/county
  compare value of Annual Average Employment
  order in desc
  assign 1-3 rank 
  leave rest within town/county as NA
  
  





# unique <- unique(employment_by_industry$`Town/County`)
# for (i in 1:length(unique)) {
#   geog <- unique[i]
#   industries <- unique(subset(employment_by_industry$`Industry Name`, employment_by_industry$`Town/County` == geog))
#   assign(paste0("industries_", i), industries)
# }
# 
# vectorlist <- list(industries_1, industries_10 , industries_100, industries_101, industries_102, industries_103, industries_104,
#                    industries_105, industries_106, industries_107, industries_108, industries_109, industries_11 , industries_110,
#                    industries_111, industries_112, industries_113, industries_114, industries_115, industries_116, industries_117,
#                    industries_118, industries_119, industries_12 , industries_120, industries_121, industries_122, industries_123,
#                    industries_124, industries_125, industries_126, industries_127, industries_128, industries_129, industries_13 ,
#                    industries_130, industries_131, industries_132, industries_133, industries_134, industries_135, industries_136,
#                    industries_137, industries_138, industries_139, industries_14 , industries_140, industries_141, industries_142,
#                    industries_143, industries_144, industries_145, industries_146, industries_147, industries_148, industries_149,
#                    industries_15 , industries_150, industries_151, industries_152, industries_153, industries_154, industries_155,
#                    industries_156, industries_157, industries_158, industries_159, industries_16 , industries_160, industries_161,
#                    industries_162, industries_163, industries_164, industries_165, industries_166, industries_167, industries_168,
#                    industries_169, industries_17 , industries_170, industries_171, industries_172, industries_173, industries_174,
#                    industries_175, industries_176, industries_177, industries_178, industries_18 , industries_19 , industries_2  ,
#                    industries_20 , industries_21 , industries_22 , industries_23 , industries_24 , industries_25 , industries_26 ,
#                    industries_27 , industries_28 , industries_29 , industries_3  , industries_30 , industries_31 , industries_32 ,
#                    industries_33 , industries_34 , industries_35 , industries_36 , industries_37 , industries_38 , industries_39 ,
#                    industries_4  , industries_40 , industries_41 , industries_42 , industries_43 , industries_44 , industries_45 ,
#                    industries_46 , industries_47 , industries_48 , industries_49 , industries_5  , industries_50 , industries_51 ,
#                    industries_52 , industries_53 , industries_54 , industries_55 , industries_56 , industries_57 , industries_58 ,
#                    industries_59 , industries_6  , industries_60 , industries_61 , industries_62 , industries_63 , industries_64 ,
#                    industries_65 , industries_66 , industries_67 , industries_68 , industries_69 , industries_7  , industries_70 ,
#                    industries_71 , industries_72 , industries_73 , industries_74 , industries_75 , industries_76 , industries_77 ,
#                    industries_78 , industries_79 , industries_8  , industries_80 , industries_81 , industries_82 , industries_83 ,
#                    industries_84 , industries_85 , industries_86 , industries_87 , industries_88 , industries_89 , industries_9  ,
#                    industries_90 , industries_91 , industries_92 , industries_93 , industries_94 , industries_95 , industries_96 ,
#                    industries_97 , industries_98 , industries_99)
# 
# 
# 
# intersect2 <- function(...) {
#   args <- list(...)
#   nargs <- length(args)
#   if(nargs <= 1) {
#     if(nargs == 1 && is.list(args[[1]])) {
#       do.call("intersect2", args[[1]])
#     } else {
#       stop("cannot evaluate intersection fewer than 2 arguments")
#     }
#   } else if(nargs == 2) {
#     intersect(args[[1]], args[[2]])
#   } else {
#     intersect(args[[1]], intersect2(args[-1]))
#   }
# }
# 
# common <- intersect2(vectorlist)



rank <- employment_by_industry[employment_by_industry$Rank == -1,]

#Reorder columns
employment_by_industry <- employment_by_industry %>% 
  select(`Town/County`, `FIPS`, `Year`, `Ownership`, `Industry Name`, `Measure Type`, `Variable`, `Value`, `Rank`)

# Write to File
write.table(
  employment_by_industry,
  file.path(getwd(), "data", "employment_by_industry.csv"),
  sep = ",",
  na = "-9999",
  row.names = F
)





#NAs should be coded as suppressed
