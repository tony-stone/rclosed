library(data.table)

nhs_postcode_directory <- fread("data-raw/geography data/Lookups/HSCIC gridlink/gridall.csv", sep = ",", header = FALSE, select = c(2:4, 37:38), colClasses = "character", na.strings="")
setnames(nhs_postcode_directory, c("postcode", "pc_start", "pc_end", "easting", "northing"))

nhs_postcode_directory[, ':=' (easting = as.integer(easting),
  northing = as.integer(northing))]

save(nhs_postcode_directory, file = "data/nhs postcode directory.Rda", compress = "xz")
