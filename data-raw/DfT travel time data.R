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

# Read in 2009 and 2011 datasets only (don't need 2015 dataset)
travel_time_data_list <- lapply(c(2009L, 2011L), function(int_year) {
  data.table(read.xlsx("data-raw/DfT Travel time data/AEFinal.xlsx", sheet = paste0("AE", int_year, "TopTen"), startRow = 1, colNames = TRUE))[, year := int_year]
})

# Bind all the datasets together
travel_time_data <- rbindlist(travel_time_data_list)

# Use more meaningful field names
setnames(travel_time_data, c("name", "totaljourneytime", "Destination"), c("LSOA", "travel_time", "destination"))

# Set data types and remove unnecessary fields
travel_time_data[, ':=' (
  travel_time = as.integer(travel_time),
  originid = NULL,
  destinationid = NULL,
  DestPostcode = NULL)]

# Save DfT data
save(travel_time_data, file = "data/dft travel times.Rda")
