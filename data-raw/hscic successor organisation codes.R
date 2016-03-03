library(data.table)

# Source: succarc 29May2015.xls opened and saved as "succarc 29May2015.*csv*"
#  File: http://systems.hscic.gov.uk/data/ods/datadownloads/data-files/succarc.zip
#  Source: http://systems.hscic.gov.uk/data/ods/datadownloads/misc
#  Retrieved: 18/05/2015

# Some NHS Acute Trust have merged during th study period we need to identify
#  mergers to uderstand catchment areas.

# Read in data ------------------------------------------------------------

# control data - intervention and control sites and trusts of interest
control_data <- fread("data-raw/control data/control file - 2016.02.24.csv", sep = ",", colClasses = "character", header = TRUE)

# successor organisation data
successor_data <- fread("data-raw/HSCIC provider codes/succarc 29May2015.csv", sep = ",", colClasses = "character",  header = FALSE)
setnames(successor_data, c("original_organisation_code", "successor_organisation_code", "succession_reason", "succession_date", "succession_indicator"))

# Narrow to time period of interest only
successor_data[successor_organisation_code %in% unique(control_data$trust_code) & succession_date >= "20070301" & succession_date < "20130901", ]

# Save this data somewhere ??
