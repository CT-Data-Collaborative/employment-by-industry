library(plyr) 
library(dplyr) 
library(datapkg)
library(readxl)
library(tidyr)

##################################################################
#
# Processing Script for Employment-by-Industry
# Created by Jenna Daly
# On 05/31/2017
#
##################################################################

#Setup environment
sub_folders <- list.files()
data_location <- grep("raw", sub_folders, value=T)
path <- (paste0(getwd(), "/", data_location))
drops <- c("")

###Create subsets

##Town Data######################################################################################################################

#grabs town xls files
town_path <- file.path(path, "town")
town_xls <- dir(town_path, pattern = "Town")

#Create empty data frame
all_towns <- data.frame(stringsAsFactors = F)
#Populate town df
for (i in 1:length(town_xls)) {
  current_file <- (read_excel(paste0(town_path, "/", town_xls[i]), sheet=1, skip=0))
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
  #Configure NAICS code column to filter on
  current_file$`NAICS Code` <- as.numeric(current_file$`NAICS Code`)
  #Set NAICS code to 100 if not assigned (place holder)
  current_file[ , 2][is.na(current_file[ , 2] ) ] = 100 
  #remove all rows that have NAICS code greater than 100
  current_file <- current_file[current_file$`NAICS Code`<=100,]
  #make copy of category column, this new column with assign ownership
  current_file$Ownership <- current_file$`Industry Name`
  #set total - all industries in ownership column to "Private Ownership"
  current_file$Ownership[current_file$Ownership == "Total - All Industries"] <- "Private Ownership"
  #set all values not equal to top level to NA
  top_levels <- c("Private Ownership", "Total Government", "Federal Government", "State Government", "Local/Municipal Government") 
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
  #set private ownership back to All for industry name = total - all industries
  current_file$Ownership[current_file$`Industry Name` == "Total - All Industries"] <- "All"
  #remove columns not needed
  current_file <- current_file[, !(names(current_file) == "NAICS Code")]
  #remove Connecticut rows
  current_file <- current_file[!current_file$`Town/County` == "State of Connecticut",]
  #bind together
  all_towns <- rbind(all_towns, current_file) 
} 

#remove all rows where industry name is NA or Industry
all_towns <- all_towns[!is.na(all_towns$`Industry Name`),]
all_towns <- all_towns[all_towns$`Industry Name` != "Industry",]

all_towns_for_rank <- all_towns

#set * to -9999 (suppressions)
all_towns[all_towns == "*"] <- -9999

#set any NAs as missing
all_towns[is.na(all_towns)] <- -6666

#set numerics
cols <- c("Number of Employers", "Annual Average Employment", "Annual Average Wage")
all_towns[cols] <- sapply(all_towns[cols],as.numeric)

#Convert to long format
cols_to_stack <- c("Number of Employers", 
                   "Annual Average Employment", 
                   "Annual Average Wage")

long_row_count = nrow(all_towns) * length(cols_to_stack)

town_long <- reshape(all_towns, 
                     varying = cols_to_stack, 
                     v.names = "Value", 
                     timevar = "Variable", 
                     times = cols_to_stack, 
                     new.row.names = 1:long_row_count,
                     direction = "long"
)

town_long$id <- NULL

#Merge in FIPS
town_fips_dp_URL <- 'https://raw.githubusercontent.com/CT-Data-Collaborative/ct-town-list/master/datapackage.json'
town_fips_dp <- datapkg_read(path = town_fips_dp_URL)
town_fips <- (town_fips_dp$data[[1]])

town_long_fips <- merge(town_long, town_fips, by.x = "Town/County", by.y = "Town",all=T)

#remove "Connecticut"
town_long_fips <- town_long_fips[ grep("Connecticut", town_long_fips$`Town/County`, invert = TRUE) , ]

#Reorder columns
town_long_fips <- town_long_fips %>% 
  select(`Town/County`, `FIPS`, `Year`, `Ownership`, `Industry Name`, `Variable`, `Value`)

#Cleanup
rm(all_towns, town_long, current_file, currentTown, currentownership)

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
  #county_naics <- current_county_df[,c(1,3)]
  current_county_df <- current_county_df[,c(1,3,4,5,7)]
  #assign `Town/County` column
  current_county_df$`Town/County` <- colnames(current_county_df)[1]
  #Assign year column
  get_year <- unique(as.numeric(unlist(gsub("[^0-9]", "", unlist(county_data[i])), "")))
  current_county_df$Year <- get_year
  #name columns, so they can be selected
  colnames(current_county_df) <- c("Naics Code", "Industry Name", "Number of Employers", "Annual Average Employment", 
                                   "Annual Average Wage", "Town/County", "Year")
  #remove blank rows (where Industry name is NA)
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
  #set County Total ownership to All
  current_county_df$Ownership[current_county_df$Ownership == "County Total"] <- "All"
  current_county_df$Ownership[current_county_df$Ownership == "Total Private"] <- "Private Ownership"  
  #set all values not equal to top level to NA
  top_levels <- c("All", "Private Ownership", "Total Government", "Total Federal Government", "Total State Government", "Total Local Government") 
  current_county_df <- current_county_df %>% mutate(Ownership = replace(Ownership, !(Ownership %in% top_levels), NA))
  #populate blank ownership rows with corresponding ownership
  currentownership = current_county_df[1,8]
  for (i in 1:nrow(current_county_df)) {
    if(is.na(current_county_df[i,8])) {
      current_county_df[i,8] <- currentownership
    } else {
      currentownership <- current_county_df[i,8]
    }
  }
  #bind all counties together
  all_counties <- rbind(all_counties, current_county_df)  
}
#relabel industry column
all_counties$`Industry Name`[all_counties$`Industry Name` == "County Total"] <- "All Industries"
all_counties$`Industry Name`[all_counties$`Industry Name` == "Total Federal Government"] <- "Federal Government"
all_counties$`Industry Name`[all_counties$`Industry Name` == "Total State Government"] <- "State Government"
all_counties$`Industry Name`[all_counties$`Industry Name` == "Total Local Government"] <- "Local/Municipal Government"

#remove columns not needed
all_counties <- all_counties[, !(names(all_counties) == "Naics Code")]

all_counties_for_rank <- all_counties
all_counties_for_rank <- all_counties_for_rank %>% 
  select(`Town/County`, `Industry Name`, `Number of Employers`, `Annual Average Employment`, `Annual Average Wage`, `Year`, `Ownership`)

#set * to -9999 (suppressions)
all_counties[all_counties == "*"] <- -9999

#set any NAs as missing
all_counties[is.na(all_counties)] <- -6666

#set digits
cols <- c("Number of Employers", "Annual Average Employment", "Annual Average Wage")
all_counties[cols] <- sapply(all_counties[cols],as.numeric)

all_counties <- all_counties %>% 
  select(`Town/County`, `Industry Name`, `Number of Employers`, `Annual Average Employment`, `Annual Average Wage`, `Year`, `Ownership`)

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

#Merge in FIPS
county_fips_dp_URL <- 'https://raw.githubusercontent.com/CT-Data-Collaborative/ct-county-list/master/datapackage.json'
county_fips_dp <- datapkg_read(path = county_fips_dp_URL)
county_fips <- (county_fips_dp$data[[1]])

county_long_fips <- merge(county_long, county_fips, by.x = "Town/County", by.y = "County", all=T)

#remove "Connecticut"
county_long_fips <- county_long_fips[ grep("Connecticut", county_long_fips$`Town/County`, invert = TRUE) , ]

#Reorder columns
county_long_fips <- county_long_fips %>% 
  select(`Town/County`, `FIPS`, `Year`, `Ownership`, `Industry Name`, `Variable`, `Value`)

#Cleanup
rm(list=ls(pattern="20"), all_counties, county_long, current_county_df, currentownership)

##State Data#####################################################################################################################

#grabs state xls files
state_path <- file.path(path, "state")
state_xls <- dir(state_path, pattern = "CT")

all_state_years <- data.frame(stringsAsFactors = F)
for (i in 1:length(state_xls)) {
  current_file <- (read_excel(paste0(state_path, "/", state_xls[i]), sheet=1, skip=0))
  #state_naics <- current_file[,c(1,3)]
  current_file <- current_file[,c(1,3,4,5,7)]
  colnames(current_file) <- c("NAICS Code", "Industry Name", "Number of Employers", "Annual Average Employment", "Annual Average Wage")
  current_file$`Town/County` <- "Connecticut"
  current_file <- current_file[-(1:6),]
  #assign year
  get_year <- unique(as.numeric(unlist(gsub("[^0-9]", "", unlist(state_xls[i])), "")))
  get_year <- get_year+2000
  current_file$Year <- get_year
  #remove rows where Industry name is blank
  current_file <- current_file %>% drop_na(`Industry Name`)  
  #Configure NAICS code column to filter on
  current_file$`NAICS Code` <- as.numeric(current_file$`NAICS Code`)
  #Set NAICS code to 100 if not assigned (place holder)
  current_file[ , 1][is.na(current_file[ , 1] ) ] = 100 
  #remove all rows that have NAICS code greater than 100
  current_file <- current_file[current_file$`NAICS Code`<=100,]
  #make copy of category column, this new column with assign ownership
  current_file$Ownership <- current_file$`Industry Name`
  #set Statewide Total ownership to All
  current_file$Ownership[current_file$Ownership == "Statewide Total"] <- "All"
  current_file$Ownership[current_file$Ownership == "Total Private"] <- "Private Ownership"   
  #set all values not equal to top level to NA
  top_levels <- c("All", "Private Ownership", "Total Government", "Total Federal Government", "Total State Government", "Total Local Government") 
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
  #bind together
  all_state_years <- rbind(all_state_years, current_file) 
}
#relabel industry column
all_state_years$`Industry Name`[all_state_years$`Industry Name` == "Statewide Total"] <- "All Industries"
all_state_years$`Industry Name`[all_state_years$`Industry Name` == "Total Federal Government"] <- "Federal Government"
all_state_years$`Industry Name`[all_state_years$`Industry Name` == "Total State Government"] <- "State Government"
all_state_years$`Industry Name`[all_state_years$`Industry Name` == "Total Local Government"] <- "Local/Municipal Government"

#remove columns not needed
current_file <- current_file[, !(names(current_file) == "NAICS Code")]

all_state_years_for_rank <- all_state_years
all_state_years_for_rank <- all_state_years_for_rank %>% 
  select(`Town/County`, `Industry Name`, `Number of Employers`, `Annual Average Employment`, `Annual Average Wage`, `Year`, `Ownership`)

#set * to -9999 (suppressions)
all_state_years[all_state_years == "*"] <- -9999

#set any NAs as missing
all_state_years[is.na(all_state_years)] <- -6666

#set digits
cols <- c("Number of Employers", "Annual Average Employment", "Annual Average Wage")
all_state_years[cols] <- sapply(all_state_years[cols],as.numeric)

#Convert to long format
cols_to_stack <- c("Number of Employers", 
                   "Annual Average Employment", 
                   "Annual Average Wage")

long_row_count = nrow(all_state_years) * length(cols_to_stack)

state_long <- reshape(all_state_years, 
                        varying = cols_to_stack, 
                        v.names = "Value", 
                        timevar = "Variable", 
                        times = cols_to_stack, 
                        new.row.names = 1:long_row_count,
                        direction = "long"
)
state_long$id <- NULL

#Add FIPS (doing this manually, because only one value)
state_long_fips <- state_long
state_long_fips$FIPS <- 9

#Reorder columns
state_long_fips <- state_long_fips %>% 
  select(`Town/County`, `FIPS`, `Year`, `Ownership`, `Industry Name`, `Variable`, `Value`)

#Cleanup
rm(all_state_years, current_file, currentownership, state_long)

#################################################################################################################

###Merge to create completed data set
employment_by_industry <- rbind(town_long_fips, county_long_fips, state_long_fips)

#Clean up
rm(town_long_fips, county_long_fips, state_long_fips)

#remove duplicates
employment_by_industry <- employment_by_industry[!duplicated(employment_by_industry), ]

#Add Measure Type
employment_by_industry$"Measure Type" <- NA
##fill in 'Measure Type' column based on criteria listed below
employment_by_industry$"Measure Type"[which(employment_by_industry$Variable %in% c("Number of Employers", 
                                                                                   "Annual Average Employment"))] <- "Number"
employment_by_industry$"Measure Type"[which(employment_by_industry$Variable %in% c("Annual Average Wage"))] <- "US Dollars"

employment_by_industry <- arrange(employment_by_industry, `Town/County`, `Year`, `Ownership`, `Variable`)

#Reorder columns
employment_by_industry <- employment_by_industry %>% 
  select(`Town/County`, `FIPS`, `Year`, `Ownership`, `Industry Name`, `Measure Type`, `Variable`, `Value`)

#round Value column
employment_by_industry$Value <- round(employment_by_industry$Value, 2)

#Create df without rank with all years
write.table(
  employment_by_industry,
  file.path(getwd(), "data", "employment_by_industry_2004_2015.csv"),
  sep = ",",
  row.names = F
)

##Define rank (dataset for Town Profiles)
#---------------------------------------------------------------------------------------------------------------------------------
#Step 1: Combine Private + Gov't owned industries for each location
#Take town/county/state data before it was converted to long format
subset_for_ranking <- rbind(all_towns_for_rank, all_counties_for_rank, all_state_years_for_rank)

#Clean up 
#rm(all_towns_for_rank, all_counties_for_rank, all_state_years_for_rank)

#Step 2: Standardize industry names
naics <- read.csv(file.path(path, "naics.csv"), header=T, stringsAsFactors = F)
standardize <- merge(subset_for_ranking, naics, by.x="Industry Name", by.y = "Industry", all.x=T)
standardize <- standardize[,-c(1,8)]
names(standardize)[names(standardize) == "Industry.Name"] <- "Industry Name"
standardize <- arrange(standardize, `Town/County`, Year)
#gets rid of some rows from 2009 (not needed for ranking anyway)
standardize <- standardize[!is.na(standardize$`Industry Name`),]

#Step 3: convert * to NA for aggregate
standardize[standardize == "*"] <- 0

subset_for_ranking_agg <- standardize %>% 
  group_by(`Town/County`, `Year`, `Industry Name`) %>% 
  summarise(`Number of Employers` = sum(as.numeric(`Number of Employers`)), 
            `Annual Average Employment` = mean(as.numeric(`Annual Average Employment`)), 
            `Annual Average Wage` = mean(as.numeric(`Annual Average Wage`)))

#Step 4: Isolate latest year in ranking df
subset_for_ranking_agg <- as.data.frame(subset_for_ranking_agg, stringsAsFactors=F)
years <- c("2014", "2015")
year_2014 <- subset_for_ranking_agg[subset_for_ranking_agg$Year %in% years[1],]
year_2015 <- subset_for_ranking_agg[subset_for_ranking_agg$Year %in% years[2],]


#Step 5: Backfill all industries to all geogs
backfill_top <- expand.grid(
  `Town/County` = unique(standardize$`Town/County`),
  `Industry Name` = unique(standardize$`Industry Name`)
)
backfill_top <- data.frame(lapply(backfill_top, as.character), stringsAsFactors=FALSE)
backfill_top <- plyr::rename(backfill_top, c("Town.County"="Town/County",
                                             "Industry.Name"="Industry Name"))

#Create list of df to loop through
dfs <- ls()[sapply(mget(ls(), .GlobalEnv), is.data.frame)]
years_for_ranking <- grep("year_", dfs, value=T)
common <- c("Construction", "Manufacturing", "Retail Trade", "Total - All Industries", "All Industries", "Total Government")
my_first_ranking_function <- function(a, b, c) {
  transform(a, ranking = ave(b, c, FUN = function(x) rank(-x, ties.method = "first")))
}
my_second_ranking_function <- function(a, b, c) {
  transform(a, ranking2 = ave(b, c, FUN = function(x) rank(x, ties.method = "max")))
}

##Loop steps:
#Step 6: if Value is NA, set to 0 (they wont be considered for top rank, but if any are in -1, they will be picked up)
#Step 7: Hardcode "-1" rank
#Step 8a: Define first function that applies rank grouped by town/county based on value of Annual avg. employment
# #Step 8b: Apply 1st function to subset df (first step in identifying ranks 1,2,3)
#Step 9: Re-assign common industy's rank to -1
#Step 10a: Define 2nd ranking function to rank 'ranking' column so all -1 ranks are assigned rank = 5 (top 5 ranks), 
#all subsequent industries are assigned based on chronological order of ranking column (ranking2 = rank: 6 = 1, 7 = 2, 8 = 3)
#Step 10b: Apply 2nd function to rank ordered rank df
#Step 10c: Reassign rank columns to establish final "Rank" values
#Step 11: Set all other industries (anything that isn't top 8) to NA
#Step 12: Finally, combine for complete df

ranking_years <- data.frame(stringsAsFactors = F)
for (i in 1:length(years_for_ranking)) {
  current_year <- get(years_for_ranking[i])
  complete_for_rank <- merge(backfill_top, current_year, by = c("Town/County", "Industry Name"), all.x=T)
  get_year <- unique(as.numeric(unlist(gsub("[^0-9]", "", unlist(years_for_ranking[i])), "")))
  complete_for_rank$Year <- get_year
  complete_for_rank[is.na(complete_for_rank)] = 0
  complete_for_rank$Rank <- NA
  complete_for_rank$Rank[which(complete_for_rank$`Industry Name` %in% common)] <- -1
  complete_for_rank <- my_first_ranking_function(complete_for_rank, complete_for_rank$"Annual Average Employment", complete_for_rank$"Town/County")
  complete_for_rank <- arrange(complete_for_rank, Town.County, ranking)
  complete_for_rank$ranking[which(complete_for_rank$`Industry.Name` %in% common)] <- -1
  complete_for_rank <- arrange(complete_for_rank, Town.County, ranking)
  complete_for_rank <- my_second_ranking_function(complete_for_rank, complete_for_rank$ranking, complete_for_rank$"Town.County")
  complete_for_rank <- arrange(complete_for_rank, Town.County, ranking2)
  complete_for_rank$Rank <- NULL
  complete_for_rank$ranking <- NULL
  #if any county or state rows have "total - all industries" remove these rows
  #if any town rows have "all industries" remove these rows
  state_and_county_rows <- filter(complete_for_rank, grepl("County|Connecticut", Town.County))
  town_rows <- filter(complete_for_rank, !grepl("County|Connecticut", Town.County))
  state_and_county_rows <- state_and_county_rows[state_and_county_rows$Industry.Name != "Total - All Industries",]
  town_rows <- town_rows[town_rows$Industry.Name != "All Industries",]
  complete_for_rank <- rbind(town_rows, state_and_county_rows)
  complete_for_rank <- arrange(complete_for_rank, Town.County, ranking2)
  complete_for_rank$ranking2[complete_for_rank$ranking2 == 6] <- -1
  complete_for_rank$ranking2[complete_for_rank$ranking2 == 7] <- 1
  complete_for_rank$ranking2[complete_for_rank$ranking2 == 8] <- 2
  complete_for_rank$ranking2[complete_for_rank$ranking2 == 9] <- 3
  complete_for_rank$ranking2[complete_for_rank$ranking2 > 3] <- NA
  complete_for_rank <- complete_for_rank[!is.na(complete_for_rank$ranking2),]
  ranking_years <- rbind(ranking_years, complete_for_rank)
}

#Step 12: Finally, rename columns in ranking subset df
ranking_years <- plyr::rename(ranking_years, c("Town.County"="Town/County",
                                               "Industry.Name"="Industry Name",
                                               "Number.of.Employers"="Number of Employers",
                                               "Annual.Average.Employment"="Annual Average Employment",
                                               "Annual.Average.Wage"="Annual Average Wage",
                                               "ranking2"="Rank"))

#Merge in FIPS
names(town_fips)[names(town_fips)=="Town"] <- "Town/County"
town_fips <- town_fips[!town_fips$`Town/County` == "Connecticut",]
names(county_fips)[names(county_fips)=="County"] <- "Town/County"
county_fips <- county_fips[!county_fips$`Town/County` == "Connecticut",]

fips <- rbind(town_fips, county_fips)

ranking_years <- merge(ranking_years, fips, by = "Town/County", all.x=T)

#set FIPS for CT
ranking_years$"FIPS"[which(ranking_years$`Town/County` %in% c("Connecticut"))] <- "09"
ranking_years <- arrange(ranking_years, `Town/County`, `Year`, `Rank`)

#Convert to long format
cols_to_stack <- c("Number of Employers", 
                   "Annual Average Employment", 
                   "Annual Average Wage")

long_row_count = nrow(ranking_years) * length(cols_to_stack)

ranking_years_long <- reshape(ranking_years, 
                              varying = cols_to_stack, 
                              v.names = "Value", 
                              timevar = "Variable", 
                              times = cols_to_stack, 
                              new.row.names = 1:long_row_count,
                              direction = "long"
)

ranking_years_long$id <- NULL

#Add Measure Type
ranking_years_long$"Measure Type" <- NA
##fill in 'Measure Type' column based on criteria listed below
ranking_years_long$"Measure Type"[which(ranking_years_long$Variable %in% c("Number of Employers", 
                                                                           "Annual Average Employment"))] <- "Number"
ranking_years_long$"Measure Type"[which(ranking_years_long$Variable %in% c("Annual Average Wage"))] <- "US Dollars"

#Arrange and round columns
ranking_years_long <- arrange(ranking_years_long, `Town/County`, `Year`, `Rank`)
ranking_years_long$Value <- round(ranking_years_long$Value, 2)

#set 0s back to -9999 to designate missing
ranking_years_long$Value[ranking_years_long$Value == 0] <- -9999

#Bring NAICS Code back in 
employment_by_industry_complete <- merge(ranking_years_long, naics, by.x = "Industry Name", by.y = "Industry.Name", all.x=T)
employment_by_industry_complete$Industry <- NULL

#remove duplicates
employment_by_industry_complete <- employment_by_industry_complete[!duplicated(employment_by_industry_complete), ]

#Reorder columns
employment_by_industry_complete <- as.data.frame(employment_by_industry_complete, stringsAsFactors=F) %>% 
  select(`Town/County`, `FIPS`, `Year`, `NAICS Code` = NAICS.Code, `Industry Name`, `Rank`, `Measure Type`, `Variable`, `Value`) %>% 
  arrange(`Town/County`, Year, Rank, `Industry Name`, Variable)

#Tests (each should have 1068 rows)
construction <- employment_by_industry_complete[employment_by_industry_complete$`Industry Name` == "Construction",]
manufacturing <- employment_by_industry_complete[employment_by_industry_complete$`Industry Name` == "Manufacturing",]
retail <- employment_by_industry_complete[employment_by_industry_complete$`Industry Name` == "Retail Trade",]
total_govt <- employment_by_industry_complete[employment_by_industry_complete$`Industry Name` == "Total Government",]
#states - 1014
total_all <- employment_by_industry_complete[employment_by_industry_complete$`Industry Name` == "Total - All Industries",]
#counties and state - 54
total_all2 <- employment_by_industry_complete[employment_by_industry_complete$`Industry Name` == "All Industries",]
rank1 <- employment_by_industry_complete[employment_by_industry_complete$`Rank` == "1",]
rank2 <- employment_by_industry_complete[employment_by_industry_complete$`Rank` == "2",]
rank3 <- employment_by_industry_complete[employment_by_industry_complete$`Rank` == "3",]
#Tests (should have 5340 rows)
rank_1 <- employment_by_industry_complete[employment_by_industry_complete$`Rank` == "-1",]
top <- unique(rank_1$`Industry Name`)

# Write to File
write.table(
  employment_by_industry_complete,
  file.path(getwd(), "data", "employment_by_industry_2014_2015_with_rank.csv"),
  sep = ",",
  row.names = F
)
