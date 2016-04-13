library(data.table)
library(openxlsx)


# Load the data for the sites we are interested in.  Includes details of site / dates / codes / etc.

# Read in data
site_data <- data.table(read.xlsx("data-raw/site data/site data - 2016.04.12.xlsx"))

# convert to correct data types
site_data[, ':=' (intervention_date = as.Date(intervention_date, origin = "1899-12-30"),
  start_date_inc = as.Date(start_date_inc, origin = "1899-12-30"),
  end_date_exc = as.Date(end_date_exc, origin = "1899-12-30"),
  is_single_ED_trust = as.logical(is_single_ED_trust),
  dft_year = as.Date(paste0(dft_year, "-01-01")))]

# merge in easting and northing of postcode
load("data/nhs postcode directory.Rda")
site_data <- merge(site_data, nhs_postcode_directory, by = "postcode", all.x = TRUE)

site_data[, pc_start := as.Date(paste0(pc_start, "01"), "%Y%m%d")]

# Check postcode data all of highest accuracy and postcode was valid from before the start of the period of interest up until now
stopifnot(all(site_data[, (positional_quality_indicator == 1L & pc_start < start_date_inc & is.na(pc_end))]))

# remove data quality check fields
site_data[, c("positional_quality_indicator", "pc_start", "pc_end") := NULL]

#save
save(site_data, file = "data/site data.Rda")
#write.csv(site_data, "D:/intervention and control site.csv", row.names = FALSE)
