library(data.table)

nhs_postcode_directory <- fread("data-raw/geography data/Lookups/HSCIC gridlink/gridall.csv", sep = ",", header = FALSE, select = c(2:4, 12, 37:38), colClasses = "character", na.strings="")
setnames(nhs_postcode_directory, c("postcode", "pc_start", "pc_end", "positional_quality_indicator", "easting", "northing"))

nhs_postcode_directory[positional_quality_indicator == " ", positional_quality_indicator := NA]

nhs_postcode_directory[, ':=' (positional_quality_indicator = as.integer(positional_quality_indicator),
  easting = as.integer(easting),
  northing = as.integer(northing))]

save(nhs_postcode_directory, file = "data/nhs postcode directory.Rda", compress = "xz")
