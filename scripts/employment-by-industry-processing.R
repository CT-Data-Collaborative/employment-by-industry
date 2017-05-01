library(dplyr)
library(datapkg)
library(readxl)
library(tidyr)

##################################################################
#
# Processing Script for Employment-by-Industry
# Created by Jenna Daly
# On 03/08/2017
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
  current_file <- (read_excel(paste0(town_path, "/", town_xls[i]), sheet=1, skip=0))
  colnames(current_file) = current_file[1, ]
  current_file <- current_file[, !(names(current_file) == "NAICS Code")]
  current_file <- current_file[, !(names(current_file) == "Suppress")]
  current_file <- current_file[, !(names(current_file) == "Total Annual Wages")]
  colnames(current_file) <- c("Town/County", "Category", "Number of Employers", 
                              "Annual Average Employment", "Annual Average Wage")
  #remove top row
  current_file <- current_file[-1,]
  #only keep populated columns (remove blank columns)
  current_file <- current_file[ , !(names(current_file) %in% drops)]
  #remove blank rows
  current_file <- current_file[rowSums(is.na(current_file)) != ncol(current_file),]
  #assign year
  get_year <- unique(as.numeric(unlist(gsub("[^0-9]", "", unlist(town_xls[i])), "")))
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
  all_towns <- rbind(all_towns, current_file) 
}

#Merge in FIPS
town_fips_dp_URL <- 'https://raw.githubusercontent.com/CT-Data-Collaborative/ct-town-list/master/datapackage.json'
town_fips_dp <- datapkg_read(path = town_fips_dp_URL)
fips <- (town_fips_dp$data[[1]])

town_long_fips <- merge(all_towns, fips, by.x = "Town/County", by.y = "Town",all=T)

#remove "Connecticut"
town_long_fips <- town_long_fips[ grep("Connecticut", town_long_fips$`Town/County`, invert = TRUE) , ]

#Reorder columns
town_long_fips <- town_long_fips %>% 
  select(`Town/County`, `FIPS`, `Year`, `Category`, `Variable`, `Value`)

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
  colnames(current_county_df) <- c("Naics Code", "Category", "Number of Employers", "Annual Average Employment", 
                                   "Total Annual Wages", "Annual Average Wage", "Average Weekly Wage", "Town/County", "Year")
  #remove blank rows
  current_county_df <- current_county_df %>% drop_na(Category)  
  #remove first row
  current_county_df <- current_county_df[-1,]
  #Configure NAICS code column to filter on
  current_county_df$`Naics Code` <- as.numeric(current_county_df$`Naics Code`)
  #Set NAICS code to 100 if not assigned (place holder)
  current_county_df[ , 1][is.na(current_county_df[ , 1] ) ] = 100 
  #remove all rows that have NAICS code greater than 100
  current_county_df <- current_county_df[current_county_df$`Naics Code`<=100,]
  #make copy of category column, this new column with assign ownership
  current_county_df$Ownership <- current_county_df$Category
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
  current_county_df <- current_county_df[, !(names(current_county_df) == "Total Annual Wages")]
  current_county_df <- current_county_df[, !(names(current_county_df) == "Average Weekly Wage")]
  
  #bind all counties together
  all_counties <- rbind(all_counties, current_county_df)  
}

#set any NAs as missing
all_counties[is.na(all_counties)] <- -6666

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
  select(`Town/County`, `FIPS`, `Year`, `Category`, `Variable`, `Value`)

#Cleanup
rm(list=ls(pattern="20"), all_counties, county_long, current_county_df, fips)


#NAs should be coded as suppressed
