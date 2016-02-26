library(data.table)
library(openxlsx)
library(lubridate)

# Source: Bespoke data extracts from ONS, received 13/01/2016.
# Data request to ONS <Mortality@ons.gsi.gov.uk> submitted on 25/09/2015 by Tony Stone <tony.stone@sheffield.ac.uk>.

# Process 16 conditions rich in avoidable deaths --------------------------

## Read in data
# Filenames for extract 1
file_names_1 <- paste0("data-raw/ONS mortality data/Extract 1/", 2007:2014, ".xlsx")

# Read in extract 1 data
extract1_2007_2014_list <- lapply(file_names_1, function(fname) { data.table( read.xlsx(fname, sheet="Sheet1", startRow=2L, colNames=FALSE) ) } )

# Bind list of data tables into single data table
extract1_2007_2014 <- rbindlist(extract1_2007_2014_list)

# set field names
field_names <- c("deaths", "year", "month", "sex", "LSOA", "age", "place_of_death", "death_underlying_cause", "death_secondary_cause")
setnames(extract1_2007_2014, field_names)

# Fix field data types for processing later
extract1_2007_2014[, ':=' (deaths = as.integer(deaths),
  year = as.character(year),
  month = as.character(month),
  sex = as.character(sex),
  place_of_death = as.character(place_of_death))]


## Label data more meaningfully
# sex (incidental note: looked at )
extract1_2007_2014[sex == "2", sex := "female"]
extract1_2007_2014[sex == "1", sex := "male"]

# place of death
extract1_2007_2014[place_of_death == "1", place_of_death := "NHS_hospital"]
extract1_2007_2014[place_of_death == "2", place_of_death := "elsewhere"]


## Consolidate date fields
extract1_2007_2014[(month != "10" & month != "11" & month != "12"), yearmonth := paste0(year, "-0", month, "-01")]
extract1_2007_2014[(month == "10" | month == "11" | month == "12"), yearmonth := paste0(year, "-", month, "-01")]

# Set date, Remove year and month fields
extract1_2007_2014[, c("yearmonth", "year", "month") := list(as.Date(fast_strptime(yearmonth, "%Y-%m-%d")), NULL, NULL )]

## age - conform to ESP2013 age bands
# combine 90-94, 95-99, and 100+ age bands
extract1_2007_2014[(age == "95-99" | age == "100+"), age := "90-94"]

# define our old and new labels
old_age_labels <- c("<1", "01-04", "05-09", "10-14", "15-19", "20-24", "25-29", "30-34",
                "35-39", "40-44", "45-49", "50-54", "55-59", "60-64", "65-69", "70-74",
                "75-79", "80-84", "85-89", "90-94")
new_age_labels <- c("[0,1)", "[1,5)", "[5,10)", "[10,15)", "[15,20)", "[20,25)", "[25,30)","[30,35)",
                "[35,40)", "[40,45)", "[45,50)", "[50,55)", "[55,60)", "[60,65)", "[65,70)", "[70,75)",
                "[75,80)", "[80,85)", "[85,90)", "[90,Inf)")

# recode age
extract1_2007_2014[, age := new_age_labels[match(age, old_age_labels)]]


# Categorise deaths -------------------------------------------------------

# Firstly, replace "Falls" by "other" as underlying cause of death for those 75+
extract1_2007_2014[((age == "[75,80)" | age == "[80,85)" | age == "[85,90)" | age == "[90,Inf)") & death_underlying_cause == "Falls"), death_underlying_cause := "other"]

# by default start with secondary cause of death
extract1_2007_2014[, cause_of_death := death_secondary_cause]

# use underlying cause if the secondary cause is "none" OR
#  if the secondary cause is "other" and the underlying cause is not one of "Self-harm", "Fall" or "RTA"
extract1_2007_2014[death_secondary_cause == "none" | (death_secondary_cause == "other" & !(death_underlying_cause %in% c("Self-harm", "Falls", "Road traffic accident"))), cause_of_death := death_underlying_cause]

# Collapse data based on newly coded cause_of_death field
extract1_2007_2014_simplified <- extract1_2007_2014[cause_of_death != "other", .(deaths = sum(deaths)), by=.(LSOA, sex, age, place_of_death, cause_of_death, yearmonth)]

# Save the data
save(extract1_2007_2014_simplified, file = "data/ons mortality (16 conditions rich in avoidable deaths).rda")

# Remove data
rm(extract1_2007_2014, extract1_2007_2014_simplified, extract1_2007_2014_list)
gc()




# Process all deaths ------------------------------------------------------

## Read in data
# Filenames for extract 2
file_names_2 <- paste0("data-raw/ONS mortality data/Extract 2/", 2007:2014, ".xlsx")

# Read in extract 2 data
extract2_2007_2014_list <- lapply(file_names_2, function(fname) { data.table( read.xlsx(fname, sheet="Sheet1", startRow=2L, colNames=FALSE) ) } )

# Bind list of data tables into single data table
extract2_2007_2014 <- rbindlist(extract2_2007_2014_list)

# set field names
field_names <- c("deaths", "year", "month", "sex", "LSOA", "age", "place_of_death", "death_underlying_cause", "death_secondary_cause")
setnames(extract2_2007_2014, field_names)

# Fix field data types for processing later
extract2_2007_2014[, ':=' (deaths = as.integer(deaths),
  year = as.character(year),
  month = as.character(month),
  sex = as.character(sex),
  place_of_death = as.character(place_of_death),
  death_secondary_cause = NULL)]


## Label data more meaningfully
# sex (incidental note: looked at )
extract2_2007_2014[sex == "2", sex := "female"]
extract2_2007_2014[sex == "1", sex := "male"]

# place of death
extract2_2007_2014[place_of_death == "1", place_of_death := "NHS_hospital"]
extract2_2007_2014[place_of_death == "2", place_of_death := "elsewhere"]


## Consolidate date fields
extract2_2007_2014[(month != "10" & month != "11" & month != "12"), yearmonth := paste0(year, "-0", month, "-01")]
extract2_2007_2014[(month == "10" | month == "11" | month == "12"), yearmonth := paste0(year, "-", month, "-01")]

# Set date, Remove year and month fields
extract2_2007_2014[, c("yearmonth", "year", "month") := list(as.Date(fast_strptime(yearmonth, "%Y-%m-%d")), NULL, NULL )]

## age - conform to ESP2013 age bands
# combine 90-94, 95-99, and 100+ age bands
extract2_2007_2014[(age == "95-99" | age == "100+"), age := "90-94"]

# define our old and new labels
old_age_labels <- c("<1", "01-04", "05-09", "10-14", "15-19", "20-24", "25-29", "30-34",
  "35-39", "40-44", "45-49", "50-54", "55-59", "60-64", "65-69", "70-74",
  "75-79", "80-84", "85-89", "90-94")
new_age_labels <- c("[0,1)", "[1,5)", "[5,10)", "[10,15)", "[15,20)", "[20,25)", "[25,30)","[30,35)",
  "[35,40)", "[40,45)", "[45,50)", "[50,55)", "[55,60)", "[60,65)", "[65,70)", "[70,75)",
  "[75,80)", "[80,85)", "[85,90)", "[90,Inf)")

# recode age
extract2_2007_2014[, age := new_age_labels[match(age, old_age_labels)]]

# Collapse data based on lsoa/sex/age/place/underlying_cause/month
extract2_2007_2014_simplified <- extract2_2007_2014[, .(deaths=sum(deaths)), by=.(LSOA, sex, age, place_of_death, death_underlying_cause, yearmonth)]

# Save the data
save(extract2_2007_2014_simplified, file = "data/ons mortality (conditions by chapter).rda")


# Check data looks as we might expect -------------------------------------
# library(ggplot2)
# # Deaths from all conditions by month for 10 year age bands
# data.check <- extract2.2007.2014[, .(date= as.Date(paste0(date.yearmonth, "-01")),Total.death = sum(deaths)), by=.(date.yearmonth, age)]
#
# dec.age.labels <- c("[0,10)", "[0,10)", "[0,10)", "[10,20)", "[10,20)", "[20,30)", "[20,30)","[30,40)",
#                     "[30,40)", "[40,50)", "[40,50)", "[50,60)", "[50,60)", "[60,70)", "[60,70)", "[70,80)",
#                     "[70,80)", "[80,90)", "[80,90)", "[90,Inf)")
#
# data.check$age.dec <- dec.age.labels[match(data.check$age, new.age.labels)]
#
# data.check <- data.check[, .(date, Total.death=sum(Total.death)), by=.(date.yearmonth, age.dec)]
#
# ggplot( data=data.check, aes(x=date, y=Total.death, colour=age.dec) ) +
#   geom_line(size=1.25) +
#   scale_x_date(  expand=c(0.01,0), name="Month", date_breaks="1 month", date_labels="%b %Y", limits=c( as.Date("2007-04-01"), as.Date("2014-03-01") )  ) +
#   scale_y_continuous("Deaths from conditions identified as rich in avoidable deaths", limits=c(0, 15000), expand=c(0,0)) +
#   scale_colour_discrete(name="Catchment area") +
#   theme(axis.text.x = element_text(face="bold", angle=90, hjust=0.0, vjust=0.3)) +
#   geom_vline(xintercept=as.numeric(as.Date("2011-04-01"))) +
#   theme(legend.position="bottom")
