#clean up naics file
town_naics <- town_naics[!duplicated(town_naics), ]
town_naics <- town_naics[!is.na(town_naics$Industry),]
town_naics <- town_naics[town_naics$Industry != "Industry",]

county_naics <- county_naics[!duplicated(county_naics), ]
colnames(county_naics) <- c("NAICS Code", "Industry")
county_naics <- county_naics[!is.na(county_naics$Industry),]
county_naics <- county_naics[county_naics$Industry != "Industry",]
county_naics_na <- county_naics[is.na(county_naics$`NAICS Code`),]
county_naics <- subset(county_naics, nchar(as.character(`NAICS Code`)) != 3)
county_naics <- rbind(county_naics, county_naics_na)


state_naics <- state_naics[!duplicated(state_naics), ]
colnames(state_naics) <- c("NAICS Code", "Industry")
state_naics <- state_naics[!is.na(state_naics$Industry),]
state_naics <- state_naics[state_naics$Industry != "Industry",]
state_naics_na <- state_naics[is.na(state_naics$`NAICS Code`),]
state_naics <- subset(state_naics, nchar(as.character(`NAICS Code`)) != 3)
state_naics <- rbind(state_naics, state_naics_na)

naics <- rbind(town_naics, county_naics, state_naics)
naics <- naics[!duplicated(naics), ]

