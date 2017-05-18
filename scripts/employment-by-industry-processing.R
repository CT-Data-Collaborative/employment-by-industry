library(dplyr) 
library(datapkg)
library(readxl)
library(tidyr)

##################################################################
#
# Processing Script for Employment-by-Industry
# Created by Jenna Daly
# On 05/10/2017
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
  
#remove all rows where industry name is NA or Industry
all_towns <- all_towns[!is.na(all_towns$`Industry Name`),]
all_towns <- all_towns[all_towns$`Industry Name` != "Industry",]

all_towns_for_rank <- all_towns

#set * to -9999 (suppressions)
all_towns[all_towns == "*"] <- -9999

#set any NAs as missing
all_towns[is.na(all_towns)] <- -6666

#set digits
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
fips <- (town_fips_dp$data[[1]])

town_long_fips <- merge(town_long, fips, by.x = "Town/County", by.y = "Town",all=T)

#remove "Connecticut"
town_long_fips <- town_long_fips[ grep("Connecticut", town_long_fips$`Town/County`, invert = TRUE) , ]

#Reorder columns
town_long_fips <- town_long_fips %>% 
  select(`Town/County`, `FIPS`, `Year`, `Ownership`, `Industry Name`, `Variable`, `Value`)

#Cleanup
rm(town_long, current_file, currentTown, currentownership, fips)

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

all_counties_for_rank <- all_counties


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
fips <- (county_fips_dp$data[[1]])

county_long_fips <- merge(county_long, fips, by.x = "Town/County", by.y = "County", all=T)

#remove "Connecticut"
county_long_fips <- county_long_fips[ grep("Connecticut", county_long_fips$`Town/County`, invert = TRUE) , ]

#Reorder columns
county_long_fips <- county_long_fips %>% 
  select(`Town/County`, `FIPS`, `Year`, `Ownership`, `Industry Name`, `Variable`, `Value`)

#Cleanup
rm(list=ls(pattern="20"), county_long, current_county_df, currentownership, fips)

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
  #bind together
  all_state_years <- rbind(all_state_years, current_file) 
}

all_state_years_for_rank <- all_state_years

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
rm(current_file, currentownership, state_long)

#################################################################################################################

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
#Step 1a: Isolate latest year
employment_by_industry$Year <- as.numeric(employment_by_industry$Year)
latest_year <- max(employment_by_industry$Year)
employment_by_industry_profiles <- employment_by_industry[employment_by_industry$Year == latest_year,]

#Step 1b: Combine Private + Gov't owned industries for each location
#Take town/county/state data before it was converted to long format
subset_for_ranking <- rbind(all_towns_for_rank, all_counties_for_rank, all_state_years_for_rank)

#convert * to NA for aggregate
subset_for_ranking[subset_for_ranking == "*"] <- 0

subset_for_ranking_agg <- subset_for_ranking %>% 
  group_by(`Town/County`, `Year`, `Industry Name`) %>% 
  summarise(`Number of Employers_sum` = sum(as.numeric(`Number of Employers`, na.rm=T)), 
            `Annual Average Employment_avg` = mean(as.numeric(`Annual Average Employment`, na.rm=T)), 
            `Annual Average Wage_avg` = mean(as.numeric(`Annual Average Wage`, na.rm=T)))

#subset_for_ranking <- employment_by_industry_profiles[employment_by_industry_profiles$Variable == "Annual Average Employment",]

max_ownership <- subset_for_ranking %>% 
  group_by(`Town/County`, `Year`, `Industry Name`) %>% 
  summarise(Value = max(Value)) 

#merge back in ownership column
max_ownership <- as.data.frame(max_ownership)
merge <- merge(max_ownership, subset_for_ranking, by = c("Town/County", "Year", "Value", "Industry Name"), all.x=T)

#Step 2a: Backfill top 5 industries to all geogs
backfill_top <- expand.grid(
  `Town/County` = unique(merge$`Town/County`),
  `Industry Name` = unique(merge$`Industry Name`)
)

complete_profiles <- merge(backfill_top, merge, by = c("Town/County", "Industry Name"), all.x=T)

#if Value is NA, set to 0 (they wont be considered for top, but if any are in -1, they will be picked up)
complete_profiles["Value"][is.na(complete_profiles["Value"] ) ] = 0

#Step 2b: Hardcode "-1" rank
common <- c("Construction", "Manufacturing", "Retail Trade", "Total Private", "Total Government")

complete_profiles$Rank <- NA
complete_profiles$Rank[which(complete_profiles$`Industry Name` %in% common)] <- -1

#Step 3a: Define first function that applies rank grouped by town/county based on value of Annual avg. employment
my_first_ranking_function <- function(a, b, c) {
  transform(a, ranking = ave(b, c, FUN = function(x) rank(-x, ties.method = "first")))
}

#Step 5: Apply 1st function to subset df (first step in identifying ranks 1,2,3)
ranking_test <- my_first_ranking_function(complete_profiles, complete_profiles$"Value", complete_profiles$"Town/County")
ranking_test_ordered <- arrange(ranking_test, Town.County, ranking)

#Step 6: Re-assign common industy's rank to -1
ranking_test_ordered$ranking[which(ranking_test_ordered$`Industry.Name` %in% common)] <- -1
ranking_test_ordered <- arrange(ranking_test_ordered, Town.County, ranking)

#Step 7: Define 2nd ranking function to rank 'ranking' column so all -1 ranks are assigned rank = 5 (top 5 ranks), 
#all subsequent industries are assigned based on chronological order of ranking column (ranking2 = rank: 6 = 1, 7 = 2, 8 = 3)
my_second_ranking_function <- function(a, b, c) {
  transform(a, ranking2 = ave(b, c, FUN = function(x) rank(x, ties.method = "max")))
}

#Step 8: Apply 2nd function to rank ordered rank df
ranking_step_two <- my_second_ranking_function(ranking_test_ordered, ranking_test_ordered$ranking, ranking_test_ordered$"Town.County")
ranking_step_two <- arrange(ranking_step_two, Town.County, ranking2)

ranking_step_two$Rank <- NULL
ranking_step_two$ranking <- NULL

#Step 9: Reassign ranking2 column to establish final "Rank" values
ranking_step_two$ranking2[ranking_step_two$ranking2 == 5] <- -1
ranking_step_two$ranking2[ranking_step_two$ranking2 == 6] <- 1
ranking_step_two$ranking2[ranking_step_two$ranking2 == 7] <- 2
ranking_step_two$ranking2[ranking_step_two$ranking2 == 8] <- 3

#Step 10: Set all other industries (anything that isn't top 8) to NA
ranking_step_two$ranking2[ranking_step_two$ranking2 > 3] <- NA
ranking_test_ordered_step_two <- arrange(ranking_step_two, Town.County, ranking2)
#remove rows where ranking2 = NA
ranking_test_ordered_step_two <- ranking_test_ordered_step_two[!is.na(ranking_test_ordered_step_two$ranking2),]

#Step 11: Rename columns in ranking subset df, so we can merge it back with the original df
names(ranking_test_ordered_step_two)[names(ranking_test_ordered_step_two) == "Town.County"] <- "Town/County"
names(ranking_test_ordered_step_two)[names(ranking_test_ordered_step_two) == "Industry.Name"] <- "Industry Name"
names(ranking_test_ordered_step_two)[names(ranking_test_ordered_step_two) == "Measure.Type"] <- "Measure Type"
names(ranking_test_ordered_step_two)[names(ranking_test_ordered_step_two) == "ranking2"] <- "Rank"

#Step 12: Merge subset df back with original df
profiles_with_rank <- merge(complete_profiles, ranking_test_ordered_step_two, 
                                          by = c("Town/County", "Industry Name", "FIPS", "Value", "Year", "Ownership", "Measure Type", "Variable", "Rank"), all.y = T)

#Step 13: redistribute Rank assignments (so all variables within a given industry get correct Rank assignments)
subset_for_backfill <- profiles_with_rank[,c(1,2,9)]
subset_for_backfill <- arrange(subset_for_backfill, `Town/County`, `Rank`)

#Step 14: Finally, merge final ranking df back with original df
profiles_with_FINAL_rank <- merge(employment_by_industry_profiles, subset_for_backfill, by = c("Town/County", "Industry Name"), all=T)
profiles_with_FINAL_rank_ordered <- arrange(profiles_with_FINAL_rank, `Town/County`, `Rank`)

profiles_with_FINAL_rank_ordered <- profiles_with_FINAL_rank_ordered[!is.na(profiles_with_FINAL_rank_ordered$Rank),]

#Clean up columns
# profiles_with_FINAL_rank_ordered$Rank.x <- NULL

#names(profiles_with_FINAL_rank_ordered)[names(profiles_with_FINAL_rank_ordered) == "Rank.y"] <- "Rank"

#Reorder columns
profiles_with_FINAL_rank_ordered <- profiles_with_FINAL_rank_ordered %>% 
  select(`Town/County`, `FIPS`, `Year`, `Ownership`, `Industry Name`, `Measure Type`, `Variable`, `Value`, `Rank`)

# Write to File
write.table(
  profiles_with_FINAL_rank_ordered,
  file.path(getwd(), "data", "employment_by_industry_2015_with_rank.csv"),
  sep = ",",
  row.names = F
)

