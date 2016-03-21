library(data.table)
library(openxlsx)
library(lubridate)


# Sources -----------------------------------------------------------------
# NOTE: All uncompressed files converted from *.xls to *.xlsx type to save space and so as to be able to use fast openxlsx library
#
# Source: 2006-2010 data (females)
# File: http://www.ons.gov.uk/ons/rel/sape/soa-mid-year-pop-est-engl-wales-exp/mid-2002-to-mid-2010-revised/rft---lsoa-unformatted-table-females.zip
# Source: http://www.ons.gov.uk/ons/publications/re-reference-tables.html?edition=tcm%3A77-285154
# Retrieved: 28/01/2016
#
# 2006-2010 data (males)
# File: http://www.ons.gov.uk/ons/rel/sape/soa-mid-year-pop-est-engl-wales-exp/mid-2002-to-mid-2010-revised/rft---lsoa-unformatted-table-males.zip
# Source: http://www.ons.gov.uk/ons/publications/re-reference-tables.html?edition=tcm%3A77-285154
# Retrieved: 28/01/2016
#
# 2011 data
# File: http://www.ons.gov.uk/ons/about-ons/business-transparency/freedom-of-information/what-can-i-request/published-ad-hoc-data/pop/april-2013/mid-2011-lsoa-syoa-population-estimates-england-and-wales.zip
# Source: http://www.ons.gov.uk/ons/about-ons/business-transparency/freedom-of-information/what-can-i-request/published-ad-hoc-data/pop/april-2013/index.html
# Retrieved: 28/01/2016
#
# 2012 data
# File: http://www.ons.gov.uk/ons/rel/sape/soa-mid-year-pop-est-engl-wales-exp/mid-2012/rft---lsoa-unformatted-table.zip
# Source: http://www.ons.gov.uk/ons/publications/re-reference-tables.html?edition=tcm%3A77-320861
# Retrieved: 28/01/2016
#
# 2013 data
# File: http://www.ons.gov.uk/ons/rel/sape/small-area-population-estimates/mid-2014-and-mid-2013/rft-lsoa-unformatted-table-2013.zip
# Source: http://www.ons.gov.uk/ons/publications/re-reference-tables.html?edition=tcm%3A77-395002
# Retrieved: 28/01/2016
#
# 2014 data
# File: http://www.ons.gov.uk/ons/rel/sape/small-area-population-estimates/mid-2014-and-mid-2013/rft-lsoa-unformatted-table-2014.zip
# Source: http://www.ons.gov.uk/ons/publications/re-reference-tables.html?edition=tcm%3A77-395002
# Retrieved: 28/01/2016
#
# 2011 rolled forward data
# File: http://www.ons.gov.uk/ons/rel/sape/soa-mid-year-pop-est-engl-wales-exp/mid-2011--census-based-/rft---mid-2011-lsoa-rolled-forward-table.zip
# Source: http://www.ons.gov.uk/ons/publications/re-reference-tables.html?edition=tcm%3A77-285629
# Retrieved: 28/01/2016

# Script to process population estimate data from 2011-based to 2001-based LSOAs.  Additionally linearly interpolates data from annual to monthly.


# Read in 2006-2010 population data ---------------------------------------

# Read in 2006 population data
males_2006 <- data.table(read.xlsx("data-raw/ONS population estimates/SAPE8DT2a-LSOA-syoa-unformatted-males-mid2002-to-mid2006.xlsx", sheet = "Mid-2006", startRow = 1, colNames = TRUE))[, year:= 2006L]
females_2006 <- data.table(read.xlsx("data-raw/ONS population estimates/SAPE8DT3a-LSOA-syoa-unformatted-females-mid2002-to-mid2006.xlsx", sheet = "Mid-2006", startRow = 1, colNames = TRUE))[, year:= 2006L]

# Read in 2007 - 2010 population data
years <- c(2007:2010)
males_2007_2010_list <- lapply(years, function(yr) { data.table(read.xlsx("data-raw/ONS population estimates/SAPE8DT2b-LSOA-syoa-unformatted-males-mid2007-to-mid2010.xlsx", sheet = paste0("Mid-", yr), startRow = 2, colNames = FALSE) )[, year := yr] })
females_2007_2010_list <- lapply(years, function(yr) { data.table(read.xlsx("data-raw/ONS population estimates/SAPE8DT3b-LSOA-syoa-unformatted-females-mid2007-to-mid2010.xlsx", sheet = paste0("Mid-", yr), startRow = 2, colNames = FALSE))[, year := yr] })

# bind lists of data tables into one data table for each sex for years 2006 to 2010
males_2006_2010 <- rbindlist(list(males_2006, rbindlist(males_2007_2010_list)))
females_2006_2010 <- rbindlist(list(females_2006, rbindlist(females_2007_2010_list)))

# add sex field
males_2006_2010[, sex := "male"]
females_2006_2010[, sex := "female"]

# combine datasets
persons_2006_2010 <- rbindlist(list(males_2006_2010, females_2006_2010))

# Remove data tables we're no longer using
rm(males_2006, females_2006, males_2007_2010_list, females_2007_2010_list, males_2006_2010, females_2006_2010)
gc()

# Remove fields relating to LA
persons_2006_2010[, c("LAD11CD", "LAD11NM") := NULL]



# Read in 2011-2014 data --------------------------------------------------

file_name <- c("mid-2011-lsoa-syoa-unformatted-file.xlsx",
  "mid-2012-lsoa-syoa-unformatted-file.xlsx",
  "SAPE16DT2-mid-2013-lsoa-syoa-estimates-unformatted.xlsx",
  "SAPE17DT2-mid-2014-lsoa-syoa-estimates-unformatted.xlsx")

file_data <- data.table(year = c(2011:2014), file_name)

males_2011_2014_list <- apply(file_data, MARGIN = 1, function(x) {
  data.table(read.xlsx(paste0("data-raw/ONS population estimates/", x["file_name"]), sheet = paste0("Mid-", x["year"], " Males"), startRow = 2, colNames = FALSE))[, year := x["year"]]
})

females_2011_2014_list <- apply(file_data, MARGIN = 1, function(x) {
  data.table(read.xlsx(paste0("data-raw/ONS population estimates/", x["file_name"]), sheet = paste0("Mid-", x["year"], " Females"), startRow = 2, colNames = FALSE))[, year := x["year"]]
})

# bind lists of data tables into one data table for each sex for years 2011 to 2014
males_2011_2014 <- rbindlist(males_2011_2014_list)
females_2011_2014 <- rbindlist(females_2011_2014_list)

# Remove data tables we're no longer using
rm(males_2011_2014_list, females_2011_2014_list)
gc()

# Remove LSOA name, add sex, and convert year field to integer
males_2011_2014[, c("X2", "sex", "year")  :=  list(NULL, "male", as.integer(year))]
females_2011_2014[, c("X2", "sex", "year")  :=  list(NULL, "female", as.integer(year))]


# Bind all population data together ---------------------------------------

# combine datasets
persons_2006_2014 <- rbindlist(list(persons_2006_2010, males_2011_2014, females_2011_2014))

# Remove data tables we're no longer using
rm(persons_2006_2010, males_2011_2014, females_2011_2014)
gc()


# Convert from single year age to grouped age -----------------------------

# reshape data to long form
persons_2006_2014_long <- melt(persons_2006_2014, id=c("LSOA11CD", "year", "sex"), measure=patterns("^m"), variable.name="age", value.name="population", variable.factor=FALSE)

# Remove data tables we have finished with
rm(persons_2006_2014)
gc()

# Transform age variable into integer
persons_2006_2014_long[age == "m90plus", age := "m90"]
persons_2006_2014_long[, age := as.integer(substr(age, 2, nchar(age)))]

# Categorise age into ESP2013 age bands
persons_2006_2014_long[, age_cat := cut(age, c(0, 1, seq(5, 95, 5)), right = FALSE)]

# Slight mod to age_cat factor labels
levels(persons_2006_2014_long$age_cat)[levels(persons_2006_2014_long$age_cat) == "[90,95)"] <- "[90,Inf)"

# Sum ages by LSOA11CD, year, sex, age_cat
persons_2006_2014_long_grouped <- persons_2006_2014_long[, .(population = sum(population1)), by=.(LSOA11CD, year, sex, age_cat)]

# Remove data tables we have finished with
rm(persons_2006_2014_long)
gc()

# Convert population estimates from LSOA11 to LSOA01 ----------------------
# - convert population estimates from 2011-census based LSOAs to 2001-census based LSOAs

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
setnames(LSOA2011_to_LSOA2001_attribution, c("LSOA11", "attribution", "LSOA01"))

# Remove non-English LSOAs
LSOA2011_to_LSOA2001_attribution <- LSOA2011_to_LSOA2001_attribution[substr(LSOA11, 1, 1) == "E",]

# Prepare to merge LSOA2011 population data into apportioning data
setkey(persons_2006_2014_long_grouped, LSOA11CD)
setkey(LSOA2011_to_LSOA2001_attribution, LSOA11)

# Merge data
LSOA2001_population_data <- persons_2006_2014_long_grouped[LSOA2011_to_LSOA2001_attribution]

# Remove data tables we have finished with
rm(persons_2006_2014_long_grouped, LSOA2011_to_LSOA2001_attribution)
gc()

# Apportion population data
LSOA2001_population_data[, population := population * attribution]

# aggregate populations based on LSOA2001, age.band, sex and year
lsoa_population_annual_data <- LSOA2001_population_data[, .(population = sum(population)), by=.(LSOA01, year, sex, age_cat)]

# Remove data tables we have finished with
rm(LSOA2001_population_data)
gc()

# Save our annual 2006 to 2014 estimates
save(lsoa_population_annual_data, file = "data/2001-census lsoa annual population estimates 2006-2014.Rda", compress = "bzip2")

# # sense check (check against ONS timeseries of England population) - DONE, MATCHES.
# lsoa_population_annual_data[, (pop = sum(population)), by = year]




# Generate monthly population estimates (by linear interpolation) -------------
# Period Jan 2007 to Mar 2014

# Read annual data if necessary
#load("data/2001-census lsoa annual population estimates 2006-2014.rda")

# Make the data wide form and give valid field names
lsoa_population_annual_data[, year := paste0("y", year)]
LSOA2001_population_data_wide <- dcast(lsoa_population_annual_data, LSOA01+sex+age_cat~year, value.var="population")

# Remove data tables we have finished with
rm(lsoa_population_annual_data)
gc()

# Function to create an expression which will add new fields in datatable linearly interpolating annual population data to monthly
addMonthlyPopulationEstimate <- function(col_name, year_a, time_point) {
  eval(parse(text = paste0("LSOA2001_population_data_wide[,", col_name, " := y", year_a, " + (y", year_a + 1, "-y", year_a, ") / 12 * ", time_point, "]")))
}

### Define the arguments to pass to the above function
# Define time points of interest. Period 1 is p200701 (Jan 2007); Period 87 is p201403 (March 2014)
periods <- c(unlist(lapply(paste0("p", 2007:2013), paste0, c(paste0("0", 1:9), 10:12))), paste0("p2014", paste0("0", 1:3)))
# Our first period is in the year of 2007, but the first period lies before the data point in that year we use the previous year as our reference year
year_ref <- 2006
# Our datapoints are for the month of June so we define a 6 month offset (from our ref month of Jan); only works because our data is ALWAYS for the same month
month_offset <- 6

# Call function to create monthly data points
for(i in 1:length(periods)) {
  addMonthlyPopulationEstimate(periods[i], floor((i-month_offset)/12) + year_ref + 1, (i%%12-month_offset)%%12)
}

# Remove annual values
LSOA2001_population_data_wide[, paste0("y", 2006:2014):=NULL]

# Convert to long form
lsoa_population_monthly_data <- melt( LSOA2001_population_data_wide, id=c("LSOA01", "sex", "age_cat"), measure=patterns("^p"), variable.name="yearmonth", value.name="population", variable.factor=FALSE)

# Remove data tables we have finished with
rm(LSOA2001_population_data_wide)
gc()

# Adjust date field
lsoa_population_monthly_data[, yearmonth := as.Date(fast_strptime(paste0(substr(yearmonth, 2, 5), "-", substr(yearmonth, 6, 7), "-01"), "%Y-%m-%d"))]

# set population field name
setnames(lsoa_population_monthly_data, "population1", "population")

# Save the monthly population data
save(lsoa_population_monthly_data, file = "data/2001-census lsoa monthly population estimates 2007-01 to 2014-03.Rda", compress = "bzip2")

# # sense check (check against ONS timeseries of England population) - DONE, MATCHES.
# lsoa_population_monthly_data[yearmonth %in% as.Date(paste0(2007:2013, "-06-01")), (pop = sum(population)), by = yearmonth]



# # Quick validation of data produced ------------------------------------------------
#
# # Look at 2011 data
# test_a <- LSOA2001_population_data_long[yearmonth==as.Date("2011-06-01"), ]
#
# # Remove data tables we have finished with
# rm(LSOA2001_population_data_long)
# gc()
#
# # Read in 2011 rolled forward data - these are 2001-census based population estimates (and based on 2001-census LSOA boundaries)
# males_2011_rf <- data.table( read.xlsx("data-raw/ONS population estimates/_validation-data/mid-2011-rolled-forward-lsoa-estimates.xlsx", sheet="Mid-2011 Males", startRow=1, colNames=TRUE) )
# females_2011_rf <- data.table( read.xlsx("data-raw/ONS population estimates/_validation-data/mid-2011-rolled-forward-lsoa-estimates.xlsx", sheet="Mid-2011 Females", startRow=2, colNames=FALSE) )
#
# # Remove MSOA field and add sex
# males_2011_rf[, c("MSOA.Code", "sex"):=list(NULL, "male")]
# females_2011_rf[, c("X2", "sex"):=list(NULL, "female")]
#
# # Bind together
# test_b <- rbindlist( list(males_2011_rf, females_2011_rf) )
#
# # Some problem with the original field names, rename fixes
# cnames <- colnames(test_b)
# cnames[1] <- "LSOA01CD"
# setnames(test_b, cnames)
#
# # reshape data to long form
# test_b <- melt(test_b, id=c("LSOA01CD", "sex"), measure=patterns("^M"), variable.name="age", value.name="population", variable.factor=FALSE)
#
# # Transform age pseudo-facor variable into integer; set sex as factor
# test_b[age == "M90plus", age := "M90"]
# test_b[, age := as.integer(substr(age, 2, nchar(age)))]
#
# # Categorise age into ESP2013 age bands
# test_b[, age_cat := as.character(cut(age, c( 0, 1, seq(5, 95, 5) ), right=FALSE))]
#
# # Slight mod to age factor labels
# test_b[age_cat == "[90,95)", age_cat := "[90,Inf)"]
#
# # Sum ages by LSOA01CD, sex, age_cat
# test_b <- test_b[, .(population_B = sum(population1)), by=.(LSOA01CD, sex, age_cat)]
#
# # Join test.a and test.B
# setkey(test_a, LSOA01, sex, age_cat)
# setkey(test_b, LSOA01CD, sex, age_cat)
# test <- test_b[test_a]
#
# # 2011-census based LSOA population data for 2011 sums to 53,107,169 for England
# # Ensure our estimated 2001-census based population in 2011 also sums to this.
# #  Check how this compares to 2011 ONS estimates rolled forward from 2001 to 2011.
# test[, .(Pop_a = sum(population), Pop_b = sum(population_B))]
#
# # Check correalation between ONS rolled forward estimates and our estimates derived from 2011 based data.
# cor(test[, .(population, population_B)])
