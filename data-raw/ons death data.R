library(data.table)
library(openxlsx)
library(lubridate)

# Source: Bespoke data extracts from ONS, received 13/01/2016.
# Data request to ONS <Mortality@ons.gsi.gov.uk> submitted on 25/09/2015 by Tony Stone <tony.stone@sheffield.ac.uk>.

# Process death data from 16 conditions rich in avoidable deaths --------------------------

## Read in data
# Filenames for extract 1
file_names_1 <- paste0("data-raw/ONS death data/Extract 1 v2/", 2007:2014, ".xlsx")

# Read in extract 1 data
extract1_2007_2014_list <- lapply(file_names_1, function(fname) { data.table( read.xlsx(fname, sheet="Sheet1", startRow=2L, colNames=FALSE) ) } )

# Bind list of data tables into single data table
extract1_2007_2014 <- rbindlist(extract1_2007_2014_list)

# set field names
field_names <- c("deaths", "year", "month", "sex", "lsoa", "age", "place_of_death", "death_underlying_cause", "death_secondary_cause")
setnames(extract1_2007_2014, field_names)

# Fix field data types for processing later
extract1_2007_2014[, ':=' (deaths = as.integer(deaths),
  year = as.character(year),
  month = as.character(month),
  sex = as.character(sex),
  place_of_death = as.character(place_of_death))]


## Label data more meaningfully
# sex
extract1_2007_2014[sex == "2", sex := "female"]
extract1_2007_2014[sex == "1", sex := "male"]

# place of death
extract1_2007_2014[place_of_death == "1", place_of_death := "nhs hospital"]
extract1_2007_2014[place_of_death == "2", place_of_death := "elsewhere"]


## Consolidate date fields
extract1_2007_2014[(month != "10" & month != "11" & month != "12"), yearmonth := paste0(year, "-0", month, "-01")]
extract1_2007_2014[(month == "10" | month == "11" | month == "12"), yearmonth := paste0(year, "-", month, "-01")]

# Set date, Remove year and month fields
extract1_2007_2014[, ':=' (yearmonth = as.Date(fast_strptime(yearmonth, "%Y-%m-%d")),
  year = NULL,
  month = NULL )]

## age
# Fix Excel date conversion for ages in 2009
extract1_2007_2014[age == "41913", age := "10-14"]
extract1_2007_2014[age == "42461", age := "01-04"]
extract1_2007_2014[age == "42618", age := "05-09"]

# prepare data for conversion to ordered factor (via integer) and conform to ESP2013 age bands
extract1_2007_2014[age == "<1", age := "0"]
extract1_2007_2014[, age_new := as.integer(substr(age, 1, 2))]
extract1_2007_2014[age == "100+", age_new := 100L]

# Create (ordered) factor variable, age_cat  - which conforms to ESP2013 age bands
extract1_2007_2014[, age_cat := cut(age_new, c(0, 1, seq(5, 105, 5)), right = FALSE)]



# # Check
# data_check <- extract1_2007_2014[, .N, by = .(age, age_cat)]
# data_check <- setorder(data_check, age_cat)
# View(data_check)

# Slight mod to age_cat factor labels
levels(extract1_2007_2014$age_cat)[levels(extract1_2007_2014$age_cat) == "[100,105)"] <- "[100,Inf)"

# Check data
# data_check <- extract1_2007_2014[, .N, by = .(death_underlying_cause, death_secondary_cause)]
# setorder(data_check, death_underlying_cause)
# View(data_check)

# Categorise deaths -------------------------------------------------------

# Firstly, replace "Falls" by "other" as underlying cause of death for those 75+
extract1_2007_2014[age_cat %in% c("[75,80)", "[80,85)", "[85,90)", "[90,95)", "[95,100)", "[100,Inf)") & death_underlying_cause == "Falls", death_underlying_cause := "other"]

# by default start with secondary cause of death
extract1_2007_2014[, cause_of_death := death_secondary_cause]

###############################################################################
# # OLD LOGIC - As interpreted from "MCRU Programme 2006-2010: The emergency and
# #  urgent care system - Final Report" BUT figures in that report suggest that
# #  this logic is not correct.
# # use underlying cause if the secondary cause is "none" OR
# #  if the secondary cause is "other" and the underlying cause is not one of "Self-harm", "Fall" or "RTA"
# extract1_2007_2014[death_secondary_cause == "none" | (death_secondary_cause == "other" & !(death_underlying_cause %in% c("Self-harm", "Falls", "Road traffic accident"))), cause_of_death := death_underlying_cause]
###############################################################################

# use underlying cause if the secondary cause is "none" OR "other"
extract1_2007_2014[death_secondary_cause == "none" | death_secondary_cause == "other", cause_of_death := death_underlying_cause]

# Collapse data based on newly coded cause_of_death field
deaths_serious_emergency_conditions <- extract1_2007_2014[cause_of_death != "other", .(deaths = sum(deaths)), by=.(lsoa, sex, age_cat, place_of_death, cause_of_death, yearmonth)]

# Standardise condition names
label_changes <- data.frame(
  old = c("Acute heart failure", "Anaphylaxis", "Asphyxiation", "Asthma", "Cardiac arrest", "Falls", "Fractured neck of femur", "Meningitis", "Myocardial Infarction", "Pregnancy and birth related", "Road traffic accident", "Ruptured aortic aneurysm", "Self-harm", "Septic shock", "Serious Head Injury", "Stroke/CVA"),
  new = c("acute heart failure", "anaphylaxis", "asphyxiation", "asthma", "cardiac arrest", "falls", "fractured neck of femur", "meningitis", "myocardial infarction", "pregnancy and birth related", "road traffic accident", "ruptured aortic aneurysm", "self harm", "septic shock", "serious head injury", "stroke cva"),
  stringsAsFactors = FALSE
)
for(condition in label_changes$old) {
  deaths_serious_emergency_conditions[cause_of_death == condition, cause_of_death := label_changes$new[label_changes$old == condition]]
}


###############################################################################
# # TEST - used to extract data to confirm actual logic used by MCRU report
# extract1_2007_2014[death_secondary_cause == "none" | death_secondary_cause == "other", cause_of_death := death_underlying_cause]
# test_data <- copy(death_serious_emergency_conditions[yearmonth >= as.Date("2007-04-01") & yearmonth < as.Date("2009-04-01"), ])
# test_data[yearmonth >= as.Date("2007-04-01") & yearmonth < as.Date("2008-04-01"), year := "20078"]
# test_data[yearmonth >= as.Date("2008-04-01") & yearmonth < as.Date("2009-04-01"), year := "20089"]
# test_data <- test_data[, .(deaths = sum(deaths)), by = .(cause_of_death, year)]
# setorder(test_data, year, cause_of_death)
# write.table(test_data, "clipboard", sep="\t")
###############################################################################

# Save the data
save(deaths_serious_emergency_conditions, file = "data/ons deaths (16 serious emergency conditions).Rda", compress = "bzip2")

load("data/ons deaths (16 serious emergency conditions).Rda")

# Remove data
rm(label_changes, deaths_serious_emergency_conditions, extract1_2007_2014, extract1_2007_2014_list)
gc()


# Process all deaths ------------------------------------------------------

## Read in data
# Filenames for extract 2
file_names_2 <- paste0("data-raw/ONS death data/Extract 2/", 2007:2014, ".xlsx")

# Read in extract 2 data
extract2_2007_2014_list <- lapply(file_names_2, function(fname) { data.table( read.xlsx(fname, sheet="Sheet1", startRow=2L, colNames=FALSE) ) } )

# Bind list of data tables into single data table
extract2_2007_2014 <- rbindlist(extract2_2007_2014_list)

# set field names
field_names <- c("deaths", "year", "month", "sex", "lsoa", "age", "place_of_death", "death_underlying_cause", "death_secondary_cause")
setnames(extract2_2007_2014, field_names)

# Fix field data types for processing later
extract2_2007_2014[, ':=' (deaths = as.integer(deaths),
  year = as.character(year),
  month = as.character(month),
  sex = as.character(sex),
  place_of_death = as.character(place_of_death),
  death_secondary_cause = NULL)]


## Label data more meaningfully
# sex
extract2_2007_2014[sex == "2", sex := "female"]
extract2_2007_2014[sex == "1", sex := "male"]

# place of death
extract2_2007_2014[place_of_death == "1", place_of_death := "nhs hospital"]
extract2_2007_2014[place_of_death == "2", place_of_death := "elsewhere"]


## Consolidate date fields
extract2_2007_2014[(month != "10" & month != "11" & month != "12"), yearmonth := paste0(year, "-0", month, "-01")]
extract2_2007_2014[(month == "10" | month == "11" | month == "12"), yearmonth := paste0(year, "-", month, "-01")]

# Set date, Remove year and month fields
extract2_2007_2014[, ':=' (yearmonth = as.Date(fast_strptime(yearmonth, "%Y-%m-%d")),
  year = NULL,
  month = NULL )]

# prepare data for conversion to ordered factor (via integer) and conform to ESP2013 age bands
extract2_2007_2014[age == "<1", age_new := "0"]
extract2_2007_2014[, age_new := as.integer(substr(age, 1, 2))]
extract2_2007_2014[age == "100+", age_new := 100L]

# Create (ordered) factor variable, age_cat  - which conforms to ESP2013 age bands
extract2_2007_2014[, age_cat := cut(age_new, c(0, 1, seq(5, 105, 5)), right = FALSE)]

# Slight mod to age_cat factor labels
levels(extract2_2007_2014$age_cat)[levels(extract2_2007_2014$age_cat) == "[100,105)"] <- "[100,Inf)"

# Collapse data based on lsoa/sex/age/place/underlying_cause/month
deaths_all_chapters <- extract2_2007_2014[yearmonth >= as.Date("2007-04-01"), .(deaths=sum(deaths)), by=.(lsoa, sex, age_cat, place_of_death, death_underlying_cause, yearmonth)]

# Save the data
save(deaths_all_chapters, file = "data/ons deaths (conditions by chapter).Rda", compress = "xz")


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
