library(data.table)
library(openxlsx)

# Source: Bespoke data provided by the Department for Transport
# <Rachel.Moyce@dft.gsi.gov.uk>, received 03/02/2016.
# Data request to DfT <SUBNATIONAL.STATS@dft.gsi.gov.uk> submitted on 30/11/2015
# by Tony Stone <tony.stone@sheffield.ac.uk>.
# NOTE:
# Provided "A&EFinal.xlsx" file contained sheetnames with ampersand ("&") which
# causes openxlsx to throw an error. Ampersands removed from sheetnames and file
# saved as "AEFinal.xlsx".

# Read in and clean up data -----------------------------------------------

# Read in datasets
years <- c(2009L, 2011L, 2015L)

travel_time_data_list <- lapply(years, function(yr) {
  data.table(read.xlsx("data-raw/DfT Travel time data/AEFinal.xlsx", sheet = paste0("AE", yr, "TopTen"), startRow = 2L, colNames = FALSE))[, year := yr]
})


# Correct for labelling error in 2015 data --------------------------------
# For 2015 dataset, DfT did not attach value labels to the EDs.
# However, originid is equal to the row number (minus 1, due to header) of the "British A&E config 2015" file I sent to DfT.

# Read in "British A&E config 2015" file
british_AE_config_2015 <- fread("data-raw/DfT Travel time data/British A&E config 2015.csv", sep = ",", header = TRUE, colClasses = "character")
british_AE_config_2015[, c("id", "easting", "northing") := list(as.numeric(row.names(british_AE_config_2015)), NULL, NULL)]

# set join key on each table
setkey(travel_time_data_list[[3]], X4)
setkey(british_AE_config_2015, id)

# Do join
travel_time_data_list[[3]] <- british_AE_config_2015[travel_time_data_list[[3]]]

# Tidy up so 2015 data has same columns and column order as 2009 and 2011 data.
travel_time_data_list[[3]][, c("X5", "X6") := NULL]
setcolorder(travel_time_data_list[[3]], c("X1", "X2", "X3", "id", "site.name", "postcode", "year"))


# Combine datasets --------------------------------------------------------

# Bind all the datasets together
travel_time_data <- rbindlist(travel_time_data_list)

# Use more meaningful field names
setnames(travel_time_data, c("origin_id", "LSOA", "travel_time", "destination_id", "destination", "destination_postcode", "year"))

# Set data types and remove unnecessary fields
travel_time_data[, ':=' (
  travel_time = as.integer(travel_time),
  origin_id = NULL,
  destination_id = NULL,
  destination_postcode = NULL)]

# Some entries have a travel time of 10^7minutes (meaning origin and destination not connected), set these to NA
travel_time_data[travel_time == 10000000, travel_time := NA]

# Save DfT data
save(travel_time_data, file = "data/dft travel times.rda")

# # Check how many LSOAs did not have any valid travel time
# # - this is due to DfT software incorrectly recording that origin (LSOA) is not connected to any specified destination (A&E / ED)
# test <- suppressWarnings(travel_time_data[, .(min_time = as.double(min(travel_time, na.rm = TRUE))), by=.(LSOA, year)])
# test[min_time == Inf, .N, by = year]
# saveRDS(test[min_time == Inf, .(LSOA, year)], "LSOAs missing travel time.Rds")
