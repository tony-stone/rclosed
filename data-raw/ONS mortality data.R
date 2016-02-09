dependecies <- c("data.table", "openxlsx", "ggplot2")
lapply(dependecies, library, character.only = T)


# Process 16 conditions rich in avoidable deaths --------------------------

# Filenames for extract 1
file.names.1 <- paste0("data-raw/ONS mortality data/Extract 1/", 2007:2014, ".xlsx")

# Read in extract 1 data
extract1.2007.2014.list <- lapply(file.names.1, function(fname) { data.table( read.xlsx(fname, sheet="Sheet1", startRow=2L, colNames=FALSE) ) } )

# Bind list of data tables into single data table
extract1.2007.2014 <- rbindlist(extract1.2007.2014.list)

# set field names
field.names <- c("deaths", "year", "month", "sex", "LSOA01", "age", "place.of.death", "death.underlying.cause", "death.secondary.cause")
setnames(extract1.2007.2014, field.names)

# Consolidate fields, fix data types
extract1.2007.2014[, c("deaths", "date.yearmonth", "sex", "place.of.death") := list(
  as.integer(deaths),
  paste0(year, "-0", month),
  as.factor(sex),
  as.factor(place.of.death) )]


# When creating date.yearmonth field above we assummed month was one character in length, fix for Oct to Dec
indexes <- (extract1.2007.2014$month == 10 | extract1.2007.2014$month == 11 | extract1.2007.2014$month == 12)
extract1.2007.2014$date.yearmonth[indexes] <- paste0(extract1.2007.2014$year[indexes], "-", extract1.2007.2014$month[indexes])

# Fix date.yearmonth data type and remove year and month fields
extract1.2007.2014[, c("date.yearmonth", "year", "month") := list(
  as.factor(date.yearmonth),
  NULL,
  NULL )]


### Relabel factor vars
# sex
levels(extract1.2007.2014$sex) <- list(male="1", female="2")

# place of death
levels(extract1.2007.2014$place.of.death) <- list(NHS_hospital="1", elsewhere="2")

### age - conform to ESP2013 age bands
# combine 90-94, 95-99, and 100+ age bands
extract1.2007.2014$age[extract1.2007.2014$age == "95-99" | extract1.2007.2014$age == "100+"] <- "90-94"

# define our old and new labels
old.age.labels <- c("<1", "01-04", "05-09", "10-14", "15-19", "20-24", "25-29", "30-34",
                "35-39", "40-44", "45-49", "50-54", "55-59", "60-64", "65-69", "70-74",
                "75-79", "80-84", "85-89", "90-94")
new.age.labels <- c("[0,1)", "[1,5)", "[5,10)", "[10,15)", "[15,20)", "[20,25)", "[25,30)","[30,35)",
                "[35,40)", "[40,45)", "[45,50)", "[50,55)", "[55,60)", "[60,65)", "[65,70)", "[70,75)",
                "[75,80)", "[80,85)", "[85,90)", "[90,Inf)")

# recode age
extract1.2007.2014$age <- new.age.labels[match(extract1.2007.2014$age, old.age.labels)]

# make age factor type
extract1.2007.2014[, age:=as.factor(age)]



# Categorise deaths -------------------------------------------------------
# NOTE: Much of this processing is not strictly necessary as we do not appear to need to look at the 16 conditions separately

# Copy the data
extract1.2007.2014.simplified <- copy(extract1.2007.2014)

# Firstly, replace "Falls" by "other" as underlying cause of death for those 75+
set(extract1.2007.2014.simplified, which(extract1.2007.2014.simplified$age %in% c("[75,80)", "[80,85)", "[85,90)", "[90,Inf)") & extract1.2007.2014.simplified$death.underlying.cause == "Falls"), "death.underlying.cause", "other")

# Remove the records for which the above replacement results in records where both causes of death are "other"
extract1.2007.2014.simplified <- extract1.2007.2014.simplified[death.underlying.cause != "other" | death.secondary.cause != "other"]

# by default start with underlying cause of death
extract1.2007.2014.simplified[, cause.of.death := death.underlying.cause]

# use secondary cause if underlying cause takes lower priority than secondary cause (and secondary cause is not "other")
logical.selection <- (extract1.2007.2014.simplified$death.underlying.cause %in% c("other", "Self-harm", "Falls", "Road traffic accident") & extract1.2007.2014.simplified$death.secondary.cause != "other")
extract1.2007.2014.simplified$cause.of.death[logical.selection] <- extract1.2007.2014.simplified$death.secondary.cause[logical.selection]

# Not required, see note at top of section
# # Collapse data based on newly coded cause.of.death field
# extract1.2007.2014.simplified <- extract1.2007.2014.simplified[, .(deaths = sum(deaths)), by=.(LSOA01, sex, age, place.of.death, cause.of.death, date.yearmonth)]

# Collapse data based on age/sex/lsoa/month
extract1.2007.2014.simplified <- extract1.2007.2014.simplified[, .(deaths=sum(deaths)), by=.(LSOA01, sex, age, place.of.death, date.yearmonth)]

# Save the data
save(extract1.2007.2014.collapsed, file = "data/ONS Mortality (16 conditions rich in avoidable deaths) data simplified.Rda")

# Remove data
rm(extract1.2007.2014, extract1.2007.2014.simplified, extract1.2007.2014.collapsed, extract1.2007.2014.list)
invisible(gc())



# Process all deaths ------------------------------------------------------

# Filenames for extract 2
file.names.2 <- paste0("data-raw/ONS mortality data/Extract 2/", 2007:2014, ".xlsx")

# Read in extract 2 data
extract2.2007.2014.list <- lapply(file.names.2, function(fname) { data.table( read.xlsx(fname, sheet="Sheet1", startRow=2L, colNames=FALSE) ) } )

# Bind list of data tables into single data table
extract2.2007.2014 <- rbindlist(extract2.2007.2014.list)

# set field names - field names defined previously
field.names <- c("deaths", "year", "month", "sex", "LSOA01", "age", "place.of.death", "death.underlying.cause", "death.secondary.cause")
setnames(extract2.2007.2014, field.names)

# Consolidate fields, fix data types, remove secondary cause
extract2.2007.2014[, c("deaths", "date.yearmonth", "sex", "place.of.death", "death.underlying.cause", "death.secondary.cause") := list(
  as.integer(deaths),
  paste0(year, "-0", month),
  as.factor(sex),
  as.factor(place.of.death),
  as.factor(death.underlying.cause),
  NULL )]


# When creating date.yearmonth field above we assummed month was one character in length, fix for Oct to Dec
indexes <- (extract2.2007.2014$month == 10 | extract2.2007.2014$month == 11 | extract2.2007.2014$month == 12)
extract2.2007.2014$date.yearmonth[indexes] <- paste0(extract2.2007.2014$year[indexes], "-", extract2.2007.2014$month[indexes])

# Fix date.yearmonth data type and remove year and month fields
extract2.2007.2014[, c("date.yearmonth", "year", "month") := list(
  as.factor(date.yearmonth),
  NULL,
  NULL )]


### Relabel factor vars
# sex
levels(extract2.2007.2014$sex) <- list(male="1", female="2")

# place of death
levels(extract2.2007.2014$place.of.death) <- list(NHS_hospital="1", elsewhere="2")

### age - conform to ESP2013 age bands
# combine 90-94, 95-99, and 100+ age bands
extract2.2007.2014$age[extract2.2007.2014$age == "95-99" | extract2.2007.2014$age == "100+"] <- "90-94"

# recode age
# define our old and new labels
old.age.labels <- c("<1", "01-04", "05-09", "10-14", "15-19", "20-24", "25-29", "30-34",
                    "35-39", "40-44", "45-49", "50-54", "55-59", "60-64", "65-69", "70-74",
                    "75-79", "80-84", "85-89", "90-94")
new.age.labels <- c("[0,1)", "[1,5)", "[5,10)", "[10,15)", "[15,20)", "[20,25)", "[25,30)","[30,35)",
                    "[35,40)", "[40,45)", "[45,50)", "[50,55)", "[55,60)", "[60,65)", "[65,70)", "[70,75)",
                    "[75,80)", "[80,85)", "[85,90)", "[90,Inf)")

extract2.2007.2014$age <- new.age.labels[match(extract2.2007.2014$age, old.age.labels)]

# make age factor type
extract2.2007.2014[, age:=as.factor(age)]

# Save the reformatted dataset
save(extract2.2007.2014, file = "data/ONS Mortality (ALL conditions by chapter) data.Rda")


# Check data looks as we might expect -------------------------------------
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
