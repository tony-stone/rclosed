# This file is rather 'hacky' at the moment will be cleaned up in the future
library(data.table)

# Load catchment areas
catchment_areas <- readRDS("D:/data/Other data/DfT catchment areas in 2009 - 2016.02.04.Rds")

# Reduce to sites of interest only
destinations_of_interest <- c("basingstoke and north hampshire", "Bishop Auckland General Hospital", "chesterfield royal", "cumberland infirmary", "diana princess of wales",
  "Hemel Hempstead Hospital", "macclesfield district general", "Newark Hospital", "north devon district", "Rochdale Infirmary", "royal albert edward infirmary",
  "royal blackburn", "salisbury district", "scarborough", "scunthorpe general", "southport and formby district general", "University Hospital of Hartlepool",
  "warwick", "west cumberland", "yeovil district")
catchment_areas_select <- catchment_areas[destination %in% destinations_of_interest, ]


# Population data ---------------------------------------------------------

# Load population data
load("data/2001-census lsoa monthly population estimates 2007-01 to 2014-03.Rda")

# merge catchment and population data
data.table::setkey(catchment_areas_select, LSOA)
data.table::setkey(LSOA2001_population_data_long, LSOA01)
LSOA2001_population_data <- catchment_areas_select[LSOA2001_population_data_long]

# Collapse population data by yearmonth and (relavant) catchment site only
LSOA2001_population_data <- LSOA2001_population_data[!is.na(destination), ]
LSOA2001_population_data <- LSOA2001_population_data[, .(population = as.integer(round(sum(population)))), by = .(yearmonth, destination)]

# remove unnecessary population data
rm(LSOA2001_population_data_long)
gc()

# Create population "measure"
LSOA2001_population_data_measure <- copy(LSOA2001_population_data)

# "formatting"
LSOA2001_population_data_measure[, ':=' (measure = "population",
  sub_measure = NA)]
setnames(LSOA2001_population_data_measure, c("destination", "population"), c("site", "value"))


# Deaths data -------------------------------------------------------------


# Load deaths data
load("data/ons deaths (16 serious emergency conditions).Rda")

# merge catchment and deaths data
data.table::setkey(catchment_areas_select, LSOA)
data.table::setkey(mortality_serious_emergency_conditions, LSOA)
deaths_serious_emergency_conditions <- catchment_areas_select[mortality_serious_emergency_conditions]

# Collapse deaths by (relavant) catchment site, yearmonth and cause (ignore lsoa/age/sex/place)
deaths_serious_emergency_conditions <- deaths_serious_emergency_conditions[!is.na(destination), ]
deaths_serious_emergency_conditions <- deaths_serious_emergency_conditions[, .(deaths = sum(deaths)), by = .(cause_of_death, yearmonth, destination)]

# Create datapoints for each month/destination/cause of death covering the measurement space (as this is not guaranteed in mortality data [no deaths = no date point])
data_points <- data.table::data.table(expand.grid(yearmonth = c(as.vector(outer(2007:2013, paste0("-", c(paste0("0", 1:9), 10:12), "-01"), paste0)), paste0("2014-0", 1:3, "-01")), destination = unique(deaths_serious_emergency_conditions$destination), cause_of_death = unique(deaths_serious_emergency_conditions$cause_of_death), stringsAsFactors = FALSE))
data_points[, yearmonth := as.Date(lubridate::fast_strptime(yearmonth, "%Y-%m-%d"))]

# Merge data so we have a data for each and every point (set to 0 where applicable)
deaths_serious_emergency_conditions <- merge(data_points, deaths_serious_emergency_conditions, by = c("yearmonth", "cause_of_death", "destination"), all = TRUE)
deaths_serious_emergency_conditions[is.na(deaths), deaths := 0]

# Create deaths "measure"
deaths_serious_emergency_conditions_measure <- copy(deaths_serious_emergency_conditions)

# "formatting"
deaths_serious_emergency_conditions_measure[, measure := "deaths"]
setnames(deaths_serious_emergency_conditions_measure, c("destination", "cause_of_death", "deaths"), c("site", "sub_measure", "value"))


# Create mortality data ---------------------------------------------------

# Merge in deaths/population data
mortality_serious_emergency_conditions_measure <- merge(deaths_serious_emergency_conditions, LSOA2001_population_data, by = c("yearmonth", "destination"), all.x = TRUE)
mortality_serious_emergency_conditions_measure[, value := deaths / population]

# label data and drop unnecessary vars
mortality_serious_emergency_conditions_measure[, ':=' (measure = "mortality rate",
  deaths = NULL,
  population= NULL)]

# "formatting"
data.table::setnames(mortality_serious_emergency_conditions_measure, c("destination", "cause_of_death"), c("site", "sub_measure"))
data.table::setcolorder(mortality_serious_emergency_conditions_measure, c("site", "yearmonth", "measure", "sub_measure", "value"))


# Combine measures --------------------------------------------------------

example_measures_data <- rbind(mortality_serious_emergency_conditions_measure, deaths_serious_emergency_conditions_measure, LSOA2001_population_data_measure)

# save
save(example_measures_data, file = "data/example measures data.Rda")
