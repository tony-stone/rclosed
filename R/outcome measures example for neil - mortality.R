# This file is rather 'hacky' at the moment will be cleaned up in the future

# Load catchment areas
catchment_areas <- readRDS("D:/data/Other data/DfT catchment areas in 2009 - 2016.02.04.Rds")

# Reduce to areas of interest only
destinations_of_interest <- c("basingstoke and north hampshire", "Bishop Auckland General Hospital", "chesterfield royal", "cumberland infirmary", "diana princess of wales",
  "Hemel Hempstead Hospital", "macclesfield district general", "Newark Hospital", "north devon district", "Rochdale Infirmary", "royal albert edward infirmary",
  "royal blackburn", "salisbury district", "scarborough", "scunthorpe general", "southport and formby district general", "University Hospital of Hartlepool",
  "warwick", "west cumberland", "yeovil district")
catchment_areas_select <- catchment_areas[destination %in% destinations_of_interest]


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



# Mortality data ----------------------------------------------------------

# Load mortality data
load("D:/Rpackages/rclosed/data/ons mortality (16 serious emergency conditions).Rda")

# merge catchment and mortality data
data.table::setkey(catchment_areas_select, LSOA)
data.table::setkey(mortality_serious_emergency_conditions, LSOA)
mortality_serious_emergency_conditions <- catchment_areas_select[mortality_serious_emergency_conditions]

# Collapse deaths by (relavant) catchment site, yearmonth and cause (ignore lsoa/age/sex/place)
mortality_serious_emergency_conditions <- mortality_serious_emergency_conditions[!is.na(destination), ]
mortality_serious_emergency_conditions_indicator <- mortality_serious_emergency_conditions[, .(deaths = sum(deaths)), by = .(cause_of_death, yearmonth, destination)]

# Create time points which fill the time period (as this is not guaranteed in mortality data [no deaths = no date point])
data_points <- data.table::data.table(expand.grid(yearmonth = c(as.vector(outer(2007:2013, paste0("-", c(paste0("0", 1:9), 10:12), "-01"), paste0)), "2014-01-01", "2014-02-01", "2014-03-01"), destination = unique(mortality_serious_emergency_conditions_indicator$destination), cause_of_death = unique(mortality_serious_emergency_conditions_indicator$cause_of_death), stringsAsFactors = FALSE))
data_points[, yearmonth := as.Date(lubridate::fast_strptime(yearmonth, "%Y-%m-%d"))]

# Merge date so we have a data for each and every point (set to 0 where applicable)
mortality_serious_emergency_conditions_indicator <- merge(data_points, mortality_serious_emergency_conditions_indicator, by = c("yearmonth", "cause_of_death", "destination"), all = TRUE)
mortality_serious_emergency_conditions_indicator[is.na(deaths), deaths := 0]

# Combine mortality and population data -----------------------------------

# Merge in mortality/population data
mortality_serious_emergency_conditions_indicator <- merge(mortality_serious_emergency_conditions_indicator, LSOA2001_population_data, by = c("yearmonth", "destination"), all.x = TRUE)
mortality_serious_emergency_conditions_indicator[, value := deaths / population]

# label data and drop unnecessary vars
mortality_serious_emergency_conditions_indicator[, c("measure", "deaths", "population") := list("mortality counts", NULL, NULL)]

# "formatting"
data.table::setnames(mortality_serious_emergency_conditions_indicator, c("destination", "cause_of_death"), c("site", "sub_measure"))
data.table::setcolorder(mortality_serious_emergency_conditions_indicator, c("site", "yearmonth", "measure", "sub_measure", "value"))
example_measures_data <- data.table::copy(mortality_serious_emergency_conditions_indicator)

# save
save(example_measures_data, file = "data/example measures data.Rda")

