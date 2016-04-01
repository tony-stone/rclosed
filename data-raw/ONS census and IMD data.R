library(data.table)
library(openxlsx)


# Indices of multiple deprivation, 2010
# Source: https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/6872/1871524.xls
# From: https://www.gov.uk/government/statistics/english-indices-of-deprivation-2010
# Retrieved: 29/01/2016
# Note: Resaved from .xls to .xlsx file for easier reading into R

# Read in data

imd_data <- data.table(read.xlsx("data-raw/DCLG/1871524.xlsx", sheet="IMD 2010", startRow = 2, colNames = FALSE))

imd_data[, paste0("X", c(2:5)) := NULL]

setnames(imd_data, c("lsoa", "imd_score", "imd_rank"))

imd_data[, ':=' (imd_rank = as.integer(imd_rank),
  imd_quintile = cut(imd_rank,
    breaks=c(quantile(imd_data$imd_rank,
      probs = seq(0, 1, 0.2),
      na.rm = FALSE, names = FALSE, type = 8)),
    labels = FALSE, include.lowest = TRUE))]

# ONS Census data on ethnicity and long-term illness from NOMIS website
# Note: 2011-census base LSOA level


# Ethnicity ---------------------------------------------------------------

# read data
ethnicity_data <- fread("data-raw/ONS census data/ONS ethnicity QS201EW_2001_NAT_LSOA - 2015-08-17.csv", sep = ",", header = FALSE, colClasses = "character", skip = 10)

# keep only columns we care about
ethnicity_data[, paste0("V", c(1, 5:9)) := NULL]

# name fields
setnames(ethnicity_data, c("LSOA11", "all_persons", "white_persons"))

# keep only English LSOAs
ethnicity_data <- ethnicity_data[substr(LSOA11, 1, 3) == "E01"]

# convert relavent columns to double
ethnicity_data[, ':=' (all_persons = as.double(all_persons),
  white_persons = as.double(white_persons))]

# Calculate the percentage non-white
ethnicity_data[, ':=' (non_white_pc = (all_persons - white_persons) / all_persons * 100,
  all_persons = NULL,
  white_persons = NULL)]



# Long-term illness -------------------------------------------------------

# read data
longterm_illness_data <- fread("data-raw/ONS census data/CSV_QS303EW_2011STATH_NAT_OA_REL_1.A.A_EN.csv", sep = ",", header = FALSE, colClasses = "character", skip = 10)

# keep only columns we care about
longterm_illness_data[, paste0("V", c(2, 4:5)) := NULL]

# name fields
setnames(longterm_illness_data, c("LSOA11", "all_persons", "no_longterm_illness"))

# keep only English LSOAs
longterm_illness_data <- longterm_illness_data[substr(LSOA11, 1, 3) == "E01"]

# convert relavent columns to double
longterm_illness_data[, ':=' (all_persons = as.double(all_persons),
  no_longterm_illness = as.double(no_longterm_illness))]

# Calculate the percentage non-white
longterm_illness_data[, ':=' (longterm_illness_pc = (all_persons - no_longterm_illness) / all_persons * 100,
  all_persons = NULL,
  no_longterm_illness = NULL)]


# Merge data --------------------------------------------------------------

census_data_2011 <- merge(ethnicity_data, longterm_illness_data, all = TRUE, by = "LSOA11")

# Convert census data from LSOA11 to LSOA01 -------------------------------

# - convert census data from 2011-census based LSOAs to 2001-census based LSOAs

# Source: http://geoconvert.mimas.ac.uk
# Retrieved: 28/01/2016
#
# Start GeoConvert -> Match One Geography to Another -> Source Geographies: Lower super output areas and data zones
# -> Target Geographies:  Lower super output areas and data zones
# -> Source: 2011 census lower super output areas and data zones; Target: 2001 census lower super output areas and data zones
# -> Generate Table -> Lookup Table
#
#
# GeoConvert Method
# - Private communication (between CensusAggregate@jisc.ac.uk & tony.stone@sheffield.ac.uk) 29/01/2015
#
# GeoConvert uses postcode unit populations from the 2011 census.  GeoConvert aggregates the post unit populations
# to (user) selected geographies.  The aggregation is achieved by identifying (populated) postcode unit centroids
# (snapped to nearest address, documented elsewhere) within the boundaries of each the geographical units for the
# selected geographies.   To achieve this, postcode unit centroids (specified by British national grid reference
#   for Britain, Irish Grid for N.I.) and all boundary data (specified similarly) are re-projected to the WGS84
# geographic coordinate system.  To calculate the fraction of the population of each unit of source geography
# lying in each intersecting unit of destination geography, the population lying in each intersecting area is
# calculated.  This intersection population is divided by the total population in the source unit geography to
# give the fraction of each unit of source geography lying in each intersecting unit of destination geography.

#Read in data
LSOA2011_to_LSOA2001_attribution <- fread("data-raw/ONS population estimates/95792208_lut.csv", sep=",", header=FALSE, colClasses=c("character", "numeric", "character"), skip=1L)
setnames(LSOA2011_to_LSOA2001_attribution, c("LSOA11", "attribution", "lsoa"))

# Remove non-English LSOAs
LSOA2011_to_LSOA2001_attribution <- LSOA2011_to_LSOA2001_attribution[substr(LSOA11, 1, 1) == "E"]

# merge data
census_data_2011 <- merge(LSOA2011_to_LSOA2001_attribution, census_data_2011, by = "LSOA11")

# Apportion census data
census_data_2011[, ':=' (non_white_pc = non_white_pc * attribution,
  longterm_illness_pc = longterm_illness_pc * attribution)]

# aggregate populations based on LSOA2001
census_data <- census_data_2011[, .(non_white_pc = sum(non_white_pc), longterm_illness_pc = sum(longterm_illness_pc)), by = lsoa]



# Merge in IMD data -------------------------------------------------------

census_data <- merge(census_data, imd_data, by = "lsoa")

save(census_data, file = "data/census data.Rda", compress ="xz")
