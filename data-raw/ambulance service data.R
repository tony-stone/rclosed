# All this needs changing to a single final dataset per service

library(data.table)
library(iotools)
library(stringi)
library(lubridate)


cleanDestinations <- function(x, hospitals = TRUE) {
  # Make lowercase
  x <- tolower(x)

  if(hospitals) {
    # Replace all other special characters with a blank
    x <- gsub("[^a-z ]+", "", x, perl = TRUE)
    # Remove "the " from start of string
    x <- sub("^the ", "", x, perl = TRUE)
    # Remove "hospital(s)" from the end
    x <- sub(" hospitals?$", "", x, perl = TRUE)
  } else {
    # Replace all other special characters with a blank (allow numbers in ward names)
    x <- gsub("[^a-z0-9 ]+", "", x, perl = TRUE)
  }

  # replace one or more consecutive whitespace character(s) or dashes with single space
  x <- gsub("[-\\s]+", " ", x, perl = TRUE)

  # Strip leading/trailing characters
  x <- trimws(x)

  return(x)
}

convertTimes <- function(t1) {
  # When the clocks go back in October the time >= 01:00 and < 02:00 is ambiguous,
  # it could be BST or GMT. We force it to be the earlier, BST.

  # When the clocks go forward in March the time between >= 01:00 and < 02:00 do not exist

  dst_start_dates <- as.Date(paste0(2008:2014, "-03-", c(30, 29, 28, 27, 25, 31, 30)))
  logical_index <- as.Date(substr(t1, 1, 10)) %in% dst_start_dates & substr(t1, 12, 13) == "01"
  t1[logical_index] <- paste0(substr(t1[logical_index], 1, 12), "2", substr(t1[logical_index], 14, 27))

  t1 <- fast_strptime(t1, format = "%Y-%m-%d %H:%M:%OS", tz = "Europe/London", lt = FALSE)

  dst_end_dates <- as.Date(paste0(2007:2014, "-10-", c(28, 26, 25, 31, 30, 28, 27, 26)))
  logical_index <- as.Date(t1) %in% dst_end_dates & as.integer(t1) %% 86400 >= 3600 & as.integer(t1) %% 86400 < 7200
  t1[logical_index] <- t1[logical_index] - 3600

  return(t1)
}

adjustDST <- function(t1, t2, t2_date_known = FALSE) {
  # t1 is known though ambiguous with the hour preceding DST end time
  # t2 does not have a well defined date or tz

  stopifnot(all(t2 < t1))

  dst_end_dates <- as.Date(paste0(2007:2014, "-10-", c(28, 26, 25, 31, 30, 28, 27, 26)))

  if(t2_date_known) {
    logical_index <- as.Date(t2 + 3600) %in% dst_end_dates & as.integer(t2 + 3600) %% 86400 < 7200
    t2[logical_index] <- t2[logical_index] + 3600
    t2[!logical_index] <- NA
  } else {
    logical_dst_start <- !dst(t2) & dst(t2 + 86400)
    logical_dst_end <- dst(t2) & !dst(t2 + 86400)

    t2[logical_dst_start] <- t2[logical_dst_start] + 82800
    t2[logical_dst_end] <- t2[logical_dst_end] + 90000
    t2[!logical_dst_start & !logical_dst_end] <- t2[!logical_dst_start & !logical_dst_end] + 86400

    t2[t2 < t1] <- NA
  }

  return(t2)
}


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
emas_amb_data[, yearmonth := as.Date(lubridate::fast_strptime(paste0("01/", substr(date_of_call, 4, 10)), format = "%d/%m/%Y", lt = FALSE))]

emas_amb_data_red <- emas_amb_data[urgency_level %in% c("Red", "Red2", "Red1", "Purple") & call_type == "Emergency", .(yearmonth, lsoa, outcome_of_incident, time_at_switchboard, time_call_answered, time_first_resource_on_scene, time_conveying_resource_on_scene, time_at_destination, time_clear)]
emas_amb_data_green_calls <- emas_amb_data[!(urgency_level %in% c("Red", "Red2", "Red1", "Purple")) & call_type == "Emergency", .(yearmonth, lsoa, outcome_of_incident)]

emas_amb_data_red[, ':=' (time_at_switchboard = convertTimes(time_at_switchboard),
  time_call_answered = convertTimes(time_call_answered),
  time_first_resource_on_scene = convertTimes(time_first_resource_on_scene),
  time_conveying_resource_on_scene = convertTimes(time_conveying_resource_on_scene),
  time_at_destination = convertTimes(time_at_destination),
  time_clear = convertTimes(time_clear))]

emas_amb_data_red <- emas_amb_data_red[!is.na(time_first_resource_on_scene)]

emas_amb_data_red[time_call_answered < time_at_switchboard, time_call_answered := adjustDST(time_at_switchboard, time_call_answered, TRUE)]
emas_amb_data_red[time_first_resource_on_scene < time_call_answered, time_first_resource_on_scene := adjustDST(time_call_answered, time_first_resource_on_scene, TRUE)]
emas_amb_data_red[time_conveying_resource_on_scene < time_call_answered, time_conveying_resource_on_scene := adjustDST(time_call_answered, time_conveying_resource_on_scene, TRUE)]
emas_amb_data_red[time_at_destination < time_conveying_resource_on_scene, time_at_destination := adjustDST(time_conveying_resource_on_scene, time_at_destination, TRUE)]
emas_amb_data_red[time_clear < time_at_destination, time_clear := adjustDST(time_at_destination, time_clear, TRUE)]

emas_amb_data_red[, ':=' (call_to_scene_any = as.integer(time_first_resource_on_scene - time_call_answered),
  call_to_scene_conveying = as.integer(time_conveying_resource_on_scene - time_call_answered),
  scene_to_dest = as.integer(time_at_destination - time_conveying_resource_on_scene),
  call_to_dest = as.integer(time_at_destination - time_call_answered),
  dest_to_clear = as.integer(time_clear - time_at_destination))]

emas_amb_data_red[call_to_scene_any < 0, call_to_scene_any := 0]
emas_amb_data_red[call_to_scene_conveying < 0, call_to_scene_conveying := 0]
emas_amb_data_red[scene_to_dest < 0, scene_to_dest := 0]
emas_amb_data_red[call_to_dest < 0, call_to_dest := 0]
emas_amb_data_red[dest_to_clear < 0, dest_to_clear := 0]


emas_amb_data_red_calls <- emas_amb_data_red[, .(yearmonth, lsoa, outcome_of_incident, call_to_scene_any, call_to_scene_conveying, scene_to_dest, call_to_dest, dest_to_clear)]

save(emas_amb_data_red_calls, file = "data/amb_red_calls_emas_data.Rda", compress = "bzip2")
save(emas_amb_data_green_calls, file = "data/amb_green_calls_emas_data.Rda", compress = "bzip2")

# ## As ambulance services do not name hospitals consistently. We must identify hospitals "by hand"
# # Export data on conveyances to hospital and match to DfT hospital names (as these cover the country)
# conveyances_by_destination <- emas_amb_data[outcome_of_incident == "1" & yearmonth < as.Date("2011-08-01"), .N, by = destination_conveyed]
# setorder(conveyances_by_destination, destination_conveyed)
# write.table(conveyances_by_destination, file = "clipboard", sep = "\t", row.names = FALSE)
#
# emas_destinations_translation <- fread("data-raw/Ambulance service data/destination translations/emas destinations translation.csv", sep = ",", header = TRUE, colClasses = "character", na.strings = "NA")
#
# emas_amb_data <- merge(emas_amb_data, emas_destinations_translation[, .(destination_conveyed, dft_name)], by = "destination_conveyed", all.x = TRUE)
#
# emas_conveyances_by_site_lsoa_month <- emas_amb_data[outcome_of_incident == "1" & yearmonth < as.Date("2011-08-01"), .N, by = .(yearmonth, dft_name, lsoa)]
#
# setnames(emas_conveyances_by_site_lsoa_month, c("N", "dft_name"), c("volume", "destination"))
#
# emas_conveyances_by_site_lsoa_month[, service := "EMAS"]
#
# # save data
# save(emas_conveyances_by_site_lsoa_month, file = "data/emas conveyances by site lsoa month.Rda", compress = "bzip2")



# YAS ---------------------------------------------------------------------

YAS_pattern <- as.character(fread("data-raw/Ambulance service data/YAS/Apr 10 - Sep 10 Data.csv", sep = "?", nrows = 1L, header = FALSE, colClasses = "character"))
YAS_num_cols <- length(gregexpr(pattern = "\t", YAS_pattern)[[1]]) + 1

yas_amb_data <- data.table(input.file("data-raw/Ambulance service data/YAS/Apr 10 - Sep 10 Data.csv",  formatter = dstrsplit, col_types = rep("character", YAS_num_cols), sep = "\t"))

yas_amb_data[, paste0("V", 38:39) := NULL]
setnames(yas_amb_data, make.names(yas_amb_data[1,]))
unique(yas_amb_data[, Journey.Type])


# NEAS --------------------------------------------------------------------

# read in data (annoyingly fixed-width with a space seperator rather than csv)
neas_src_filepaths <- paste0("data-raw/Ambulance service data/NEAS/ClosED_", c(paste0("0", 7:9), as.character(10:12), "13dec14"), ".csv")

# in this lapply we:
#   i:    Get second line of each dataset (which has dashes seperated by spaces indicating the fixed widths)
#   ii:   Find the positions of the spaces between the dashes - the field widths - (prefix this vector with 1 [start at beginning of line] and affix with -1 [for last col read to end of line])
#   iii:  Read in the data and chunk into the columns of fixed width as found above.
neas_amb_data_list <- lapply(neas_src_filepaths, function(x) {
  NEAS_pattern <- as.character( fread(x, sep="¬", nrows=1L, header=FALSE, stringsAsFactors=FALSE, skip=1, colClasses="character") )
  col_ends <- c( 0L, gregexpr(pattern=" ", NEAS_pattern)[[1]], 0L )
  return(fread(x, sep="¬", header=FALSE, skip=2)[, lapply(1:(length(col_ends)-1L), function(i) { stri_sub(V1, col_ends[i]+1L, col_ends[i+1L]-1L) } ) ])
})

# Combine the various years of data into a single datasets
neas_amb_data <- rbindlist(neas_amb_data_list)

rm(neas_amb_data_list)
gc()

# Strip trailing spaces of all but last variable (which was the only non-fixed width variable)
neas_amb_data[, paste0("V", 1:25) := list( sub("\\s+$", "", V1), sub("\\s+$", "", V2), sub("\\s+$", "", V3), sub("\\s+$", "", V4),
  sub("\\s+$", "", V5), sub("\\s+$", "", V6), sub("\\s+$", "", V7), sub("\\s+$", "", V8),
  sub("\\s+$", "", V9), sub("\\s+$", "", V10), sub("\\s+$", "", V11), sub("\\s+$", "", V12),
  sub("\\s+$", "", V13), sub("\\s+$", "", V14), sub("\\s+$", "", V15), sub("\\s+$", "", V16),
  sub("\\s+$", "", V17), sub("\\s+$", "", V18), sub("\\s+$", "", V19), sub("\\s+$", "", V20),
  sub("\\s+$", "", V21), sub("\\s+$", "", V22), sub("\\s+$", "", V23), sub("\\s+$", "", V24),
  sub("\\s+$", "", V25) )]

# Set "Unknown", "NULL", and "" to NA
for (k in seq_along(neas_amb_data))  {
  set(neas_amb_data, i = which(neas_amb_data[[k]] == "Unknown" | neas_amb_data[[k]] == "NULL" | neas_amb_data[[k]] == ""), j = k, value = NA)
}

# Set field names
field_names <- c("anonymous_id", "call_source", "date_of_call", "call_type",
  "age", "sex", "within_service_area", "postcode_district", "lsoa",
  "time_at_switchboard", "time_call_answered", "time_chief_complaint",
  "time_first_vehicle_assigned",
  "urgency_level", "chief_complaint", "time_first_resource_on_scene",
  "time_conveying_resource_on_scene", "time_at_destination", "time_clear",
  "first_resource_type", "conveying_resource_type", "hospital_id",
  "hospital_name", "hospital_type", "outcome_of_incident", "cancel_reason")
setnames(neas_amb_data, field_names)

# Create year-month field [date type]
neas_amb_data[, ':=' (yearmonth = as.Date(fast_strptime(paste0(substr(date_of_call, 1, 7), "-01"), format = "%Y-%m-%d", lt = FALSE)),
  year = substr(date_of_call, 1, 4))]

neas_amb_data_red <- neas_amb_data[call_type == "Emergency" & urgency_level == "A", .(yearmonth, lsoa, outcome_of_incident, time_at_switchboard, time_call_answered, time_first_resource_on_scene, time_conveying_resource_on_scene, time_at_destination, time_clear)]

neas_amb_data_red[, ':=' (time_at_switchboard = convertTimes(paste(date_of_call, time_at_switchboard, sep = " ")),
  time_call_answered = convertTimes(paste(date_of_call, time_call_answered, sep = " ")),
  time_first_resource_on_scene = convertTimes(paste(date_of_call, time_first_resource_on_scene, sep = " ")),
  time_conveying_resource_on_scene = convertTimes(time_conveying_resource_on_scene),
  time_at_destination = convertTimes(paste(date_of_call, time_at_destination, sep = " ")),
  time_clear = convertTimes(time_clear))]

neas_amb_data_red <- neas_amb_data_red[!is.na(time_first_resource_on_scene)]

neas_amb_data_red[time_call_answered < time_at_switchboard, time_call_answered := adjustDST(time_at_switchboard, time_call_answered)]
neas_amb_data_red[time_first_resource_on_scene < time_call_answered, time_first_resource_on_scene := adjustDST(time_call_answered, time_first_resource_on_scene)]
neas_amb_data_red[time_conveying_resource_on_scene < time_call_answered, time_conveying_resource_on_scene := adjustDST(time_call_answered, time_conveying_resource_on_scene, TRUE)]
neas_amb_data_red[time_at_destination < time_conveying_resource_on_scene, time_at_destination := adjustDST(time_conveying_resource_on_scene, time_at_destination)]
neas_amb_data_red[time_clear < time_at_destination, time_clear := adjustDST(time_at_destination, time_clear, TRUE)]


neas_amb_data_red[, ':=' (call_to_scene_any = as.integer(time_first_resource_on_scene - time_call_answered),
  call_to_scene_conveying = as.integer(time_conveying_resource_on_scene - time_call_answered),
  scene_to_dest = as.integer(time_at_destination - time_conveying_resource_on_scene),
  call_to_dest = as.integer(time_at_destination - time_call_answered),
  dest_to_clear = as.integer(time_clear - time_at_destination))]

neas_amb_data_red[call_to_scene_any < 0, call_to_scene_any := 0]
neas_amb_data_red[call_to_scene_conveying < 0, call_to_scene_conveying := 0]
neas_amb_data_red[scene_to_dest < 0, scene_to_dest := 0]
neas_amb_data_red[call_to_dest < 0, call_to_dest := 0]
neas_amb_data_red[dest_to_clear < 0, dest_to_clear := 0]

neas_amb_data_red_calls <- neas_amb_data_red[, .(yearmonth, lsoa, outcome_of_incident, call_to_scene_any, call_to_scene_conveying, scene_to_dest, call_to_dest, dest_to_clear)]

neas_amb_data_green_calls <- neas_amb_data[urgency_level  != "A" & call_type == "Emergency", .(yearmonth, lsoa, outcome_of_incident)]

save(neas_amb_data_red_calls, file = "data/amb_red_calls_neas_data.Rda", compress = "bzip2")
save(neas_amb_data_green_calls, file = "data/amb_green_calls_neas_data.Rda", compress = "bzip2")



# NWAS --------------------------------------------------------------------

nwas_src_filepaths <- paste0("data-raw/Ambulance service data/NWAS/closED study - NWAS - 20", c(paste0("0", 7:9), 10:13), c(paste0("0", 8:9), 10:14), c(rep("v2", 5), rep("", 2)), ".csv")

nwas_amb_data_list <- lapply(nwas_src_filepaths, function(filepath) {
  return(fread(filepath, sep = ",", header = TRUE, na.strings = c("NULL", "Unknown", ""), colClasses = "character"))
})

# Standardise field names
field_names <- c("anonymous_id", "call_source", "date_of_call", "call_type",
  "age", "sex", "within_service_area", "postcode_district", "lsoa",
  "time_at_switchboard", "time_call_answered", "time_chief_complaint",
  "time_first_vehicle_assigned", "earliest_timestamp_recorded",
  "urgency_level", "dispatch_code", "time_first_resource_on_scene",
  "time_conveying_resource_on_scene", "time_at_destination", "time_clear",
  "first_resource_type", "conveying_resource_type", "destination_conveyed",
  "hospital_ward_conveyed", "outcome_of_incident", "outcome_of_incident_QA", "dead_on_scene")
setnames(nwas_amb_data, field_names)

nwas_amb_data[, outcome_of_incident_QA := NULL]

# Change date field format
nwas_amb_data[, yearmonth := as.Date(lubridate::fast_strptime(paste0("01-", match(substr(date_of_call, 4, 6), month.abb), "-", substr(date_of_call, 8, 11)), format = "%d-%m-%Y", lt = FALSE))]

nwas_amb_data_red <- nwas_amb_data[urgency_level %in% c("RED", "RED2", "RED1", "PURPLE") & call_type == "Emergency", .(yearmonth, lsoa, outcome_of_incident, time_at_switchboard, time_call_answered, time_first_resource_on_scene, time_conveying_resource_on_scene, time_at_destination, time_clear)]
nwas_amb_data_green_calls <- nwas_amb_data[!(urgency_level %in% c("RED", "RED2", "RED1", "PURPLE")) & call_type == "Emergency", .(yearmonth, lsoa, outcome_of_incident)]

nwas_amb_data_red[, ':=' (time_at_switchboard = convertTimes(time_at_switchboard),
  time_call_answered = convertTimes(time_call_answered),
  time_first_resource_on_scene = convertTimes(time_first_resource_on_scene),
  time_conveying_resource_on_scene = convertTimes(time_conveying_resource_on_scene),
  time_at_destination = convertTimes(time_at_destination),
  time_clear = convertTimes(time_clear))]

nwas_amb_data_red <- nwas_amb_data_red[!is.na(time_first_resource_on_scene)]

nwas_amb_data_red[time_call_answered < time_at_switchboard, time_call_answered := adjustDST(time_at_switchboard, time_call_answered, TRUE)]
nwas_amb_data_red[time_first_resource_on_scene < time_call_answered, time_first_resource_on_scene := adjustDST(time_call_answered, time_first_resource_on_scene, TRUE)]
nwas_amb_data_red[time_conveying_resource_on_scene < time_call_answered, time_conveying_resource_on_scene := adjustDST(time_call_answered, time_conveying_resource_on_scene, TRUE)]
nwas_amb_data_red[time_at_destination < time_conveying_resource_on_scene, time_at_destination := adjustDST(time_conveying_resource_on_scene, time_at_destination, TRUE)]
nwas_amb_data_red[time_clear < time_at_destination, time_clear := adjustDST(time_at_destination, time_clear, TRUE)]

nwas_amb_data_red[, ':=' (call_to_scene_any = as.integer(time_first_resource_on_scene - time_call_answered),
  call_to_scene_conveying = as.integer(time_conveying_resource_on_scene - time_call_answered),
  scene_to_dest = as.integer(time_at_destination - time_conveying_resource_on_scene),
  call_to_dest = as.integer(time_at_destination - time_call_answered),
  dest_to_clear = as.integer(time_clear - time_at_destination))]

nwas_amb_data_red[call_to_scene_any < 0, call_to_scene_any := 0]
nwas_amb_data_red[call_to_scene_conveying < 0, call_to_scene_conveying := 0]
nwas_amb_data_red[scene_to_dest < 0, scene_to_dest := 0]
nwas_amb_data_red[call_to_dest < 0, call_to_dest := 0]
nwas_amb_data_red[dest_to_clear < 0, dest_to_clear := 0]


nwas_amb_data_red_calls <- nwas_amb_data_red[, .(yearmonth, lsoa, outcome_of_incident, call_to_scene_any, call_to_scene_conveying, scene_to_dest, call_to_dest, dest_to_clear)]

save(nwas_amb_data_red_calls, file = "data/amb_red_calls_nwas_data.Rda", compress = "bzip2")
save(nwas_amb_data_green_calls, file = "data/amb_green_calls_nwas_data.Rda", compress = "bzip2")
