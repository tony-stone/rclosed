library(data.table)

# Load the data for the sites we are interested in.  Includes details of site / dates / codes / etc.

# Read in data
site_data <- fread(input = "data-raw/site data/site data - 2016.03.18.csv", sep = ",", header = TRUE, colClasses = "character")

# convert to correct data types
site_data[, ':=' (intervention_date = as.Date(intervention_date, format = "%d/%m/%Y"),
  start_date_inc = as.Date(start_date_inc, format = "%d/%m/%Y"),
  end_date_exc = as.Date(end_date_exc, format = "%d/%m/%Y"),
  is_intervention = as.logical(is_intervention),
  is_single_ED_trust = as.logical(is_single_ED_trust),
  dft_year = as.Date(paste0(dft_year, "-01-01")))]

#save
save(site_data, file = "data/site data.Rda")
