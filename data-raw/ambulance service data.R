library(data.table)
library(iotools)

# EMAS --------------------------------------------------------------------

emas_amb_data <- fread("data-raw/Ambulance service data/EMAS/ClosED.csv", sep = ",", header = TRUE, colClasses = "character", na.strings = "NULL")

# Standardise field names
field_names <- c("anonymous_id", "call_source", "date_of_call", "call_type",
  "age", "sex", "within_service_area", "postcode_district", "lsoa",
  "time_at_switchboard", "time_call_answered", "time_chief_complaint",
  "time_first_vehicle_assigned", "earliest_timestamp_recorded",
  "urgency_level", "dispatch_code", "time_first_resource_on_scene",
  "time_conveying_resource_on_scene", "time_at_destination", "time_clear",
  "first_resource_type", "conveying_resource_type", "destination_conveyed",
  "hospital_ward_conveyed", "outcome_of_incident", "dead_on_scene")
setnames(emas_amb_data, field_names)

# Change date field format
emas_amb_data[, date_of_call := paste0(substr(date_of_call, 7, 10), "-", substr(date_of_call, 4, 5), "-", substr(date_of_call, 1, 2))]

# Create year-month field [date type]
emas_amb_data[, yearmonth := as.Date(lubridate::fast_strptime(paste0(substr(date_of_call, 1, 7), "-01"), format = "%Y-%m-%d"))]

# ## As ambulance services do not name hospitals consistently. We must identify hospitals "by hand"
# # Export data on conveyances to hospital and match to DfT hospital names (as these cover the country)
# conveyances_by_destination <- emas_amb_data[outcome_of_incident == "1" & yearmonth < as.Date("2011-08-01"), .N, by = destination_conveyed]
# setorder(conveyances_by_destination, destination_conveyed)
# write.table(conveyances_by_destination, file = "clipboard", sep = "\t", row.names = FALSE)

emas_destinations_translation <- fread("data-raw/Ambulance service data/destination translations/emas destinations translation.csv", sep = ",", header = TRUE, colClasses = "character", na.strings = "NA")

emas_amb_data <- merge(emas_amb_data, emas_destinations_translation[, .(destination_conveyed, dft_name)], by = "destination_conveyed", all.x = TRUE)

emas_conveyances_by_site_lsoa_month <- emas_amb_data[outcome_of_incident == "1" & yearmonth < as.Date("2011-08-01"), .N, by = .(yearmonth, dft_name, lsoa)]

setnames(emas_conveyances_by_site_lsoa_month, c("N", "dft_name"), c("volume", "destination"))

emas_conveyances_by_site_lsoa_month[, service := "EMAS"]

# save data
save(emas_conveyances_by_site_lsoa_month, file = "data/emas conveyances by site lsoa month.Rda", compress = "bzip2")



# YAS ---------------------------------------------------------------------

YAS_pattern <- as.character(fread("data-raw/Ambulance service data/YAS/Apr 10 - Sep 10 Data.csv", sep = "?", nrows = 1L, header = FALSE, colClasses = "character"))
YAS_num_cols <- length(gregexpr(pattern = "\t", YAS_pattern)[[1]]) + 1

yas_amb_data <- data.table(input.file("data-raw/Ambulance service data/YAS/Apr 10 - Sep 10 Data.csv",  formatter = dstrsplit, col_types = rep("character", YAS_num_cols), sep = "\t"))

yas_amb_data[, paste0("V", 38:39) := NULL]
setnames(yas_amb_data, make.names(yas_amb_data[1,]))
unique(yas_amb_data[, Journey.Type])


