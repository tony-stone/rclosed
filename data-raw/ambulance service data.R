# All this needs changing to a single final dataset per service

library(data.table)
library(readxl)
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

timeExcelToStr <- function(t1) {
  return(format(as.POSIXct(as.double(t1) * 86400, tz = "GMT", origin = "1899-12-30"), format = "%Y-%m-%d %H:%M:%S"))
}

convertTimes <- function(t1) {
  # When the clocks go back in October the time >= 01:00 and < 02:00 is ambiguous,
  # it could be BST or GMT. We force it to be the earlier, BST.

  # When the clocks go forward in March the time between >= 01:00 and < 02:00 (GMT or BST) do not exist

  # Who knew (non-relativistic) times could be so annoying?!

  t1[t1 == "NULL"] <- NA

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
  # We expect t2 to be after t1, however it appears not to.  If we have reason to beleive this is due to a change in DST status we correct otherwise
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

rm(emas_amb_data, emas_amb_data_red)

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




# EoE ---------------------------------------------------------------------
# PROBLEMS - data requires winsorizing - some of it is bonkers - days or years for a vehicle to arrive on scene


# source data filenames
eoe_src_filepaths <- paste0("data-raw/Ambulance service data/EoE/closED data extract - ", c("Mar 2007", paste0("Apr ", 2008:2013)), " - Mar ", 2008:2014, ".txt")

# get headers
eoe_col_names <- as.character(fread(eoe_src_filepaths[1], sep = ",", header = FALSE, colClasses = "character", skip = 6, nrows = 1))

# Load all the data
eoe_amb_data_list <- lapply(eoe_src_filepaths, function(filepath) {
  data <- fread(filepath, sep = "`", header = FALSE, colClasses = "character", skip = 6)

  # ensure columns consistent throughout files
  stopifnot(data[1, V1] == paste0(eoe_col_names, collapse = ","))

  last_valid_row <- nrow(data)
  if(data[last_valid_row] == "Warning: Null value is eliminated by an aggregate or other SET operation.") {
    last_valid_row <- last_valid_row - 1
  }
  return(data[2:last_valid_row])
})

# Put all the data together
eoe_amb_data_1col <- rbindlist(eoe_amb_data_list)

# remove all quotes
eoe_amb_data_1col[, V1 := gsub("\"", "", V1, fixed = TRUE)]

# Quote "destination conveyed" field (field 23)
eoe_amb_data_1col[, cleaned_record := sub("^((?:[^,]*,){22})(.*)((?:,[^,]*){3})$", "\\1\"\\2\"\\3", V1, perl = TRUE)]

#Read data as data table
eoe_amb_data <- data.table(iotools::dstrsplit(eoe_amb_data_1col[, cleaned_record], col_types = rep("character", 26), sep = ",", quote = "\""))

# remove data no longer required
rm(eoe_amb_data_list, eoe_amb_data_1col)
gc()

# Standardise field names
field_names <- c("anonymous_id", "X_cad_system", "call_source", "date_of_call", "call_type",
  "age", "sex", "within_service_area", "postcode_district", "lsoa",
  "time_at_switchboard", "time_call_answered", "time_chief_complaint",
  "time_first_vehicle_assigned",
  "urgency_level", "dispatch_code", "time_first_resource_on_scene",
  "time_conveying_resource_on_scene", "time_at_destination", "time_clear",
  "first_resource_type", "conveying_resource_type", "destination_conveyed",
  "X_csd_only", "X_incident_disposition", "outcome_of_incident")
setnames(eoe_amb_data, field_names)

# Change date field format
eoe_amb_data[, yearmonth := as.Date(lubridate::fast_strptime(paste0(substr(date_of_call, 1, 7), "-01"), format = "%Y-%m-%d", lt = FALSE))]



#
#
# load("data/catchment area set final.Rda")
# lsoas <- unique(catchment_area_set_final$lsoa)
# rel_calls <- copy(eoe_amb_data[lsoa %in% lsoas & urgency_level %in% c("R1", "R2") & call_type == "Emergency" & outcome_of_incident == "1", .(X_cad_system, yearmonth, time_call_answered, time_chief_complaint, time_first_resource_on_scene)])
#
# rel_calls[time_call_answered == "NULL", time_call_answered := NA]
# rel_calls[time_first_resource_on_scene == "NULL", time_first_resource_on_scene := NA]
# rel_calls[time_chief_complaint == "NULL", time_chief_complaint := NA]
#
# rel_calls[, ':=' (time_call_answered1 = convertTimes(time_call_answered),
#   time_first_resource_on_scene1 = convertTimes(time_first_resource_on_scene))]
#
# rel_calls[, call_to_scene_any := as.integer(time_first_resource_on_scene1 - time_call_answered1)]
#
# rel_calls[call_to_scene_any < 0, call_to_scene_any := NA]
#
# rel_calls[, c2s_pct := as.double(quantile(call_to_scene_any, probs = 0.8, na.rm = TRUE, type = 8)[1]), by = yearmonth]
#
# rel_calls[call_to_scene_any <= c2s_pct, call_to_scene_valid := call_to_scene_any]
# rel_calls[call_to_scene_any > c2s_pct, call_to_scene_valid := NA]
#
#
# bla <- rel_calls[, .(.N, valid_ans_pc = round(sum(!is.na(time_call_answered)) / length(time_call_answered) * 100, 1), valid_scene_pc = round(sum(!is.na(time_first_resource_on_scene)) / length(time_first_resource_on_scene) * 100, 1), valid_chief_pc = round(sum(!is.na(time_chief_complaint)) / length(time_chief_complaint) * 100, 1), valid_call_to_scene_any  = round(sum(!is.na(call_to_scene_any)) / length(call_to_scene_any) * 100, 1), mean_call_to_scene_any = round(mean(call_to_scene_any, na.rm = TRUE) / 60), mean_call_to_scene_valid = round(mean(call_to_scene_valid, na.rm = TRUE) / 60), median_call_to_scene_any = round(median(call_to_scene_any, na.rm = TRUE) / 60), bs_call_to_scene_any = sum(call_to_scene_any > 3600, na.rm = TRUE) / length(call_to_scene_any) * 100), by = .(yearmonth)]
#
#
# rel_calls[, ':=' (valid_ans = !is.na(time_call_answered),
#   valid_scene = !is.na(time_first_resource_on_scene))]
# bla <- rel_calls[, .(.N, valid_ans_pc = round(sum(valid_ans) / length(valid_ans) * 100, 1), valid_scene_pc = round(sum(valid_scene) / length(valid_scene) * 100, 1)), by = .(X_cad_system, yearmonth)]
#
# bla[, calls := sum(N), by = yearmonth]
# bla[, pc := round(N / calls * 100, 1)]
#
#
#
# data <- copy(eoe_amb_data_red[, .(yearmonth, call_to_scene_any1)])
#
# data[call_to_scene_any1 < 0, call_to_scene_any1 := -1]
# data[call_to_scene_any1 > 86400, call_to_scene_any1 := 86401]
#
# ggplot2::ggplot(data, ggplot2::aes(call_to_scene_any1)) +
# #  ggplot2::facet_wrap(~ yearmonth, ncol = 2, scales = "fixed", shrink = TRUE, as.table = TRUE, drop = TRUE) +
#   ggplot2::geom_histogram() +
#   ggplot2::scale_x_continuous(limits = c(-2, 86402))




eoe_amb_data_red <- eoe_amb_data[urgency_level %in% c("R1", "R2") & call_type == "Emergency", .(yearmonth, lsoa, outcome_of_incident, time_at_switchboard, time_call_answered, time_first_resource_on_scene, time_conveying_resource_on_scene, time_at_destination, time_clear)]
eoe_amb_data_green_calls <- eoe_amb_data[!(urgency_level %in% c("R1", "R2")) & call_type == "Emergency", .(yearmonth, lsoa, outcome_of_incident)]

eoe_amb_data_red[, ':=' (time_at_switchboard = convertTimes(time_at_switchboard),
  time_call_answered = convertTimes(time_call_answered),
  time_first_resource_on_scene = convertTimes(time_first_resource_on_scene),
  time_conveying_resource_on_scene = convertTimes(time_conveying_resource_on_scene),
  time_at_destination = convertTimes(time_at_destination),
  time_clear = convertTimes(time_clear))]

eoe_amb_data_red <- eoe_amb_data_red[!is.na(time_first_resource_on_scene)]

eoe_amb_data_red[time_call_answered < time_at_switchboard, time_call_answered := adjustDST(time_at_switchboard, time_call_answered, TRUE)]
eoe_amb_data_red[time_first_resource_on_scene < time_at_switchboard, time_first_resource_on_scene := adjustDST(time_at_switchboard, time_first_resource_on_scene, TRUE)]
eoe_amb_data_red[time_conveying_resource_on_scene < time_call_answered, time_conveying_resource_on_scene := adjustDST(time_call_answered, time_conveying_resource_on_scene, TRUE)]
eoe_amb_data_red[time_at_destination < time_conveying_resource_on_scene, time_at_destination := adjustDST(time_conveying_resource_on_scene, time_at_destination, TRUE)]
eoe_amb_data_red[time_clear < time_at_destination, time_clear := adjustDST(time_at_destination, time_clear, TRUE)]

# call_to_scene_any1 = as.integer(time_first_resource_on_scene - time_at_switchboard),
eoe_amb_data_red[, ':=' (call_to_scene_any = as.integer(time_first_resource_on_scene - time_call_answered),
  call_to_scene_conveying = as.integer(time_conveying_resource_on_scene - time_call_answered),
  scene_to_dest = as.integer(time_at_destination - time_conveying_resource_on_scene),
  call_to_dest = as.integer(time_at_destination - time_call_answered),
  dest_to_clear = as.integer(time_clear - time_at_destination))]

eoe_amb_data_red[call_to_scene_any < 0, call_to_scene_any := 0]
eoe_amb_data_red[call_to_scene_conveying < 0, call_to_scene_conveying := 0]
eoe_amb_data_red[scene_to_dest < 0, scene_to_dest := 0]
eoe_amb_data_red[call_to_dest < 0, call_to_dest := 0]
eoe_amb_data_red[dest_to_clear < 0, dest_to_clear := 0]


eoe_amb_data_red_calls <- eoe_amb_data_red[, .(yearmonth, lsoa, outcome_of_incident, call_to_scene_any, call_to_scene_conveying, scene_to_dest, call_to_dest, dest_to_clear)]


rm(eoe_amb_data, eoe_amb_data_red)

# **************************************# **************************************# **************************************# **************************************# **************
# Quite a lot of data quality issues - need to check what calls we can use - poor coverage < April 2008
#eoe_amb_data_red_calls[, .(b = mean(call_to_scene_any, na.rm = TRUE), n = sum(!is.na(call_to_scene_any)), x = sum(!is.na(call_to_scene_any)) / length(call_to_scene_any) * 100), by = yearmonth]

# **************************************# **************************************# **************************************# **************************************# **************




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

neas_amb_data_red <- neas_amb_data[call_type == "Emergency" & urgency_level == "A", .(yearmonth, lsoa, outcome_of_incident, date_of_call, time_at_switchboard, time_call_answered, time_first_resource_on_scene, time_conveying_resource_on_scene, time_at_destination, time_clear)]

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

rm(neas_amb_data, neas_amb_data_red)





# NWAS --------------------------------------------------------------------

nwas_src_filepaths <- paste0("data-raw/Ambulance service data/NWAS/closED study - NWAS - 20", c(paste0("0", 7:9), 10:13), c(paste0("0", 8:9), 10:14), c(rep("v2", 5), rep("", 2)), ".csv")

nwas_amb_data_list <- lapply(nwas_src_filepaths, function(filepath) {
  return(fread(filepath, sep = ",", header = TRUE, na.strings = c("NULL", "Unknown", ""), colClasses = "character"))
})

nwas_amb_data <- rbindlist(nwas_amb_data_list)

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

rm(nwas_amb_data, nwas_amb_data_red)




# Combine data - part 1 ---------------------------------------------------

load("data/site data.Rda")
services <- sort(tolower(unique(site_data$ambulance_service)))


amb_data_red_calls_list <- lapply(services[1:4], function(serv) {
  return(get(paste0(serv, "_amb_data_red_calls"))[, ambulance_service := serv])
})
amb_data_red_calls_part1 <- rbindlist(amb_data_red_calls_list)
save(amb_data_red_calls_part1, file = "data/amb_data_red_calls_part1.Rda", compress = "xz")


rm(amb_data_red_calls_list, amb_data_red_calls_part1)
gc()


amb_data_green_calls_list <- lapply(services[1:4], function(serv) {
  return(get(paste0(serv, "_amb_data_green_calls"))[, ambulance_service := serv])
})
amb_data_green_calls_part1 <- rbindlist(amb_data_green_calls_list)

save(amb_data_green_calls_part1, file = "data/amb_data_green_calls_part1.Rda", compress = "xz")

rm(amb_data_green_calls_list, amb_data_green_calls_part1)

rm(list = paste0(rep(services[1:4], each = 2), "_amb_data_", c("red", "green"), "_calls"))
gc()

# SCAS --------------------------------------------------------------------


scas_amb_data <- fread("data-raw/Ambulance service data/SCAS/ClosedDataset_Test_SCAS.csv", sep = ",", header = TRUE, colClasses = "character")
setnames(scas_amb_data, make.names(colnames(scas_amb_data)))
# Standardise field names
field_names <- c("anonymous_id", "call_source", "call_type",
  "age", "sex", "within_service_area", "postcode_district", "lsoa",
  "time_at_switchboard", "time_call_answered", "time_chief_complaint",
  "time_first_vehicle_assigned", "earliest_timestamp_recorded",
  "urgency_level", "dispatch_code", "time_first_resource_on_scene",
  "time_conveying_resource_on_scene", "time_at_destination", "time_clear",
  "destination_conveyed", "destination_ward", "outcome_of_incident", "dead_on_scene")
setnames(scas_amb_data, field_names)

scas_amb_data[, yearmonth := time_at_switchboard]

scas_amb_data[is.na(yearmonth), yearmonth := earliest_timestamp_recorded]
scas_amb_data[is.na(yearmonth), yearmonth := time_call_answered]
scas_amb_data[is.na(yearmonth), yearmonth := time_chief_complaint]
scas_amb_data[is.na(yearmonth), yearmonth := time_first_vehicle_assigned]

stopifnot(all(!is.na(scas_amb_data$yearmonth)))

# Change date field format
scas_amb_data[, yearmonth := as.Date(lubridate::fast_strptime(paste0(substr(yearmonth, 1, 7), "-01"), format = "%Y-%m-%d", lt = FALSE))]

scas_amb_data_red <- scas_amb_data[urgency_level %in% c("Red1", "Red2") & call_type == "Emergency", .(yearmonth, lsoa, outcome_of_incident, time_at_switchboard, time_call_answered, time_first_resource_on_scene, time_conveying_resource_on_scene, time_at_destination, time_clear)]
scas_amb_data_green_calls <- scas_amb_data[!(urgency_level %in% c("Red1", "Red2")) & call_type == "Emergency", .(yearmonth, lsoa, outcome_of_incident)]

scas_amb_data_red[, ':=' (time_at_switchboard = convertTimes(time_at_switchboard),
  time_call_answered = convertTimes(time_call_answered),
  time_first_resource_on_scene = convertTimes(time_first_resource_on_scene),
  time_conveying_resource_on_scene = convertTimes(time_conveying_resource_on_scene),
  time_at_destination = convertTimes(time_at_destination),
  time_clear = convertTimes(time_clear))]

scas_amb_data_red <- scas_amb_data_red[!is.na(time_first_resource_on_scene)]

scas_amb_data_red[time_call_answered < time_at_switchboard, time_call_answered := adjustDST(time_at_switchboard, time_call_answered, TRUE)]
scas_amb_data_red[time_first_resource_on_scene < time_call_answered, time_first_resource_on_scene := adjustDST(time_call_answered, time_first_resource_on_scene, TRUE)]
scas_amb_data_red[time_conveying_resource_on_scene < time_call_answered, time_conveying_resource_on_scene := adjustDST(time_call_answered, time_conveying_resource_on_scene, TRUE)]
scas_amb_data_red[time_at_destination < time_conveying_resource_on_scene, time_at_destination := adjustDST(time_conveying_resource_on_scene, time_at_destination, TRUE)]
scas_amb_data_red[time_clear < time_at_destination, time_clear := adjustDST(time_at_destination, time_clear, TRUE)]

scas_amb_data_red[, ':=' (call_to_scene_any = as.integer(time_first_resource_on_scene - time_call_answered),
  call_to_scene_conveying = as.integer(time_conveying_resource_on_scene - time_call_answered),
  scene_to_dest = as.integer(time_at_destination - time_conveying_resource_on_scene),
  call_to_dest = as.integer(time_at_destination - time_call_answered),
  dest_to_clear = as.integer(time_clear - time_at_destination))]

scas_amb_data_red[call_to_scene_any < 0, call_to_scene_any := 0]
scas_amb_data_red[call_to_scene_conveying < 0, call_to_scene_conveying := 0]
scas_amb_data_red[scene_to_dest < 0, scene_to_dest := 0]
scas_amb_data_red[call_to_dest < 0, call_to_dest := 0]
scas_amb_data_red[dest_to_clear < 0, dest_to_clear := 0]


scas_amb_data_red_calls <- scas_amb_data_red[, .(yearmonth, lsoa, outcome_of_incident, call_to_scene_any, call_to_scene_conveying, scene_to_dest, call_to_dest, dest_to_clear)]

rm(scas_amb_data, scas_amb_data_red)

# SWAS --------------------------------------------------------------------

swas_amb_data <- fread("data-raw/Ambulance service data/SWAS/160902 closED.csv", sep = ",", header = FALSE, colClasses = "character", na.strings = "NULL")

# Remove the byte order mark from start of file - microsoft, huh.
swas_amb_data[1, V1 := sub("ï»¿", "", swas_amb_data[1, V1], fixed = TRUE)]

# Standardise field names
field_names <- c("anonymous_id", "call_source", "call_type",
  "age", "sex", "within_service_area", "postcode_district", "lsoa",
  "time_at_switchboard", "time_call_answered", "time_chief_complaint",
  "time_first_vehicle_assigned",
  "unknown", "urgency_level", "dispatch_code", "time_first_resource_on_scene",
  "time_conveying_resource_on_scene", "time_at_destination", "time_clear",
  "first_resource_type", "conveying_resource_type", "destination_conveyed",
  "destination_ward", "outcome_of_incident", "dead_on_scene", "swas_division")
setnames(swas_amb_data, field_names)

# Create month of call field
swas_amb_data[, yearmonth := as.Date(lubridate::fast_strptime(paste0(substr(time_at_switchboard, 1, 7), "-01"), format = "%Y-%m-%d", lt = FALSE))]

swas_amb_data_red <- swas_amb_data[substr(urgency_level, 1, 3) == "Red" & call_type == "Emergency", .(yearmonth, lsoa, outcome_of_incident, time_at_switchboard, time_call_answered, time_first_resource_on_scene, time_conveying_resource_on_scene, time_at_destination, time_clear)]
swas_amb_data_green_calls <- swas_amb_data[substr(urgency_level, 1, 3) != "Red" & call_type == "Emergency", .(yearmonth, lsoa, outcome_of_incident)]

swas_amb_data_red[, ':=' (time_at_switchboard = convertTimes(time_at_switchboard),
  time_call_answered = convertTimes(time_call_answered),
  time_first_resource_on_scene = convertTimes(time_first_resource_on_scene),
  time_conveying_resource_on_scene = convertTimes(time_conveying_resource_on_scene),
  time_at_destination = convertTimes(time_at_destination),
  time_clear = convertTimes(time_clear))]

swas_amb_data_red <- swas_amb_data_red[!is.na(time_first_resource_on_scene)]

swas_amb_data_red[time_call_answered < time_at_switchboard, time_call_answered := adjustDST(time_at_switchboard, time_call_answered, TRUE)]
swas_amb_data_red[time_first_resource_on_scene < time_call_answered, time_first_resource_on_scene := adjustDST(time_call_answered, time_first_resource_on_scene, TRUE)]
swas_amb_data_red[time_conveying_resource_on_scene < time_call_answered, time_conveying_resource_on_scene := adjustDST(time_call_answered, time_conveying_resource_on_scene, TRUE)]
swas_amb_data_red[time_at_destination < time_conveying_resource_on_scene, time_at_destination := adjustDST(time_conveying_resource_on_scene, time_at_destination, TRUE)]
swas_amb_data_red[time_clear < time_at_destination, time_clear := adjustDST(time_at_destination, time_clear, TRUE)]

swas_amb_data_red[, ':=' (call_to_scene_any = as.integer(time_first_resource_on_scene - time_call_answered),
  call_to_scene_conveying = as.integer(time_conveying_resource_on_scene - time_call_answered),
  scene_to_dest = as.integer(time_at_destination - time_conveying_resource_on_scene),
  call_to_dest = as.integer(time_at_destination - time_call_answered),
  dest_to_clear = as.integer(time_clear - time_at_destination))]

swas_amb_data_red[call_to_scene_any < 0, call_to_scene_any := 0]
swas_amb_data_red[call_to_scene_conveying < 0, call_to_scene_conveying := 0]
swas_amb_data_red[scene_to_dest < 0, scene_to_dest := 0]
swas_amb_data_red[call_to_dest < 0, call_to_dest := 0]
swas_amb_data_red[dest_to_clear < 0, dest_to_clear := 0]


swas_amb_data_red_calls <- swas_amb_data_red[, .(yearmonth, lsoa, outcome_of_incident, call_to_scene_any, call_to_scene_conveying, scene_to_dest, call_to_dest, dest_to_clear)]

rm(swas_amb_data, swas_amb_data_red)

# WMAS --------------------------------------------------------------------

wmas_src_filepaths <- paste0("data-raw/Ambulance service data/WMAS/", c(2009, 2010, 2012, 2013), "_", c(10, 11, 13, 14), ".xlsx")

#Load 2009/2010/2012/2013 data (extra column in 2011 data)
wmas_amb_data_list <- lapply(wmas_src_filepaths, read_excel, col_types = rep("text", 28), na = "NULL")

#Load 2011 data
wmas_amb_data_2011 <- data.table(read_excel("data-raw/Ambulance service data/WMAS/2011_12.xlsx", col_types = rep("text", 29), na = "NULL"))
wmas_amb_data_2011[, xx := NULL]

# Combine the various years of data into a single datasets
wmas_amb_data <- rbindlist(c(wmas_amb_data_list, list(wmas_amb_data_2011)))

rm(wmas_amb_data_list, wmas_amb_data_2011)
gc()

# Standardise field names
field_names <- c("anonymous_id", "XXX_responses", "XXX_transports", "date_of_call", "call_source",
  "within_service_area", "XXX_wmas_area", "postcode_district", "lsoa", "XXX_location_type",
  "call_type", "urgency_level", "dispatch_code", "age", "sex",
  "time_at_switchboard", "time_call_answered", "time_chief_complaint",
  "time_first_vehicle_assigned", "time_first_resource_on_scene",
  "time_conveying_resource_on_scene", "time_at_destination", "time_clear",
  "destination_conveyed", "hospital_ward_conveyed", "outcome_of_incident",
  "conveying_resource_type", "first_resource_type")
setnames(wmas_amb_data, field_names)

# Change date field format
wmas_amb_data[, date_of_call := as.Date(as.integer(date_of_call), origin="1899-12-30")]
wmas_amb_data[, yearmonth := as.Date(lubridate::fast_strptime(format(date_of_call, format = "%Y-%m-01"), format = "%Y-%m-%d", lt = FALSE))]

wmas_amb_data_red <- wmas_amb_data[urgency_level %in% c("A", "Red 1", "Red 2") & call_type == "Emergency", .(yearmonth, lsoa, outcome_of_incident, time_at_switchboard, time_call_answered, time_first_resource_on_scene, time_conveying_resource_on_scene, time_at_destination, time_clear)]
wmas_amb_data_green_calls <- wmas_amb_data[!(urgency_level %in% c("A", "Red 1", "Red 2")) & call_type == "Emergency", .(yearmonth, lsoa, outcome_of_incident)]

# Deal with Excel time formats

wmas_amb_data_red[, ':=' (time_at_switchboard = timeExcelToStr(time_at_switchboard),
  time_call_answered = timeExcelToStr(time_call_answered),
  time_first_resource_on_scene = timeExcelToStr(time_first_resource_on_scene),
  time_conveying_resource_on_scene = timeExcelToStr(time_conveying_resource_on_scene),
  time_at_destination = timeExcelToStr(time_at_destination),
  time_clear = timeExcelToStr(time_clear))]

wmas_amb_data_red[, ':=' (time_at_switchboard = convertTimes(time_at_switchboard),
  time_call_answered = convertTimes(time_call_answered),
  time_first_resource_on_scene = convertTimes(time_first_resource_on_scene),
  time_conveying_resource_on_scene = convertTimes(time_conveying_resource_on_scene),
  time_at_destination = convertTimes(time_at_destination),
  time_clear = convertTimes(time_clear))]

wmas_amb_data_red <- wmas_amb_data_red[!is.na(time_first_resource_on_scene)]

wmas_amb_data_red[time_call_answered < time_at_switchboard, time_call_answered := adjustDST(time_at_switchboard, time_call_answered, TRUE)]
wmas_amb_data_red[time_first_resource_on_scene < time_call_answered, time_first_resource_on_scene := adjustDST(time_call_answered, time_first_resource_on_scene, TRUE)]
wmas_amb_data_red[time_conveying_resource_on_scene < time_call_answered, time_conveying_resource_on_scene := adjustDST(time_call_answered, time_conveying_resource_on_scene, TRUE)]
wmas_amb_data_red[time_at_destination < time_conveying_resource_on_scene, time_at_destination := adjustDST(time_conveying_resource_on_scene, time_at_destination, TRUE)]
wmas_amb_data_red[time_clear < time_at_destination, time_clear := adjustDST(time_at_destination, time_clear, TRUE)]

wmas_amb_data_red[, ':=' (call_to_scene_any = as.integer(time_first_resource_on_scene - time_call_answered),
  call_to_scene_conveying = as.integer(time_conveying_resource_on_scene - time_call_answered),
  scene_to_dest = as.integer(time_at_destination - time_conveying_resource_on_scene),
  call_to_dest = as.integer(time_at_destination - time_call_answered),
  dest_to_clear = as.integer(time_clear - time_at_destination))]

wmas_amb_data_red[call_to_scene_any < 0, call_to_scene_any := 0]
wmas_amb_data_red[call_to_scene_conveying < 0, call_to_scene_conveying := 0]
wmas_amb_data_red[scene_to_dest < 0, scene_to_dest := 0]
wmas_amb_data_red[call_to_dest < 0, call_to_dest := 0]
wmas_amb_data_red[dest_to_clear < 0, dest_to_clear := 0]


wmas_amb_data_red_calls <- wmas_amb_data_red[, .(yearmonth, lsoa, outcome_of_incident, call_to_scene_any, call_to_scene_conveying, scene_to_dest, call_to_dest, dest_to_clear)]

rm(wmas_amb_data, wmas_amb_data_red)

# YAS ---------------------------------------------------------------------

# all we can get from this data are call volumes and response times.
YAS_pattern <- fread("data-raw/Ambulance service data/YAS/Call data 19.09.07 - 31.04.14.csv", sep = "`", header = FALSE, colClasses = "character")

# How many rows?
YAS_total_rows <- nrow(YAS_pattern)

# Remove the byte order mark from start of file - microsoft, huh.
YAS_top_line <- data.table(sub("ï»¿", "", YAS_pattern[1], fixed = TRUE))
YAS_pattern <- rbind(YAS_top_line, YAS_pattern[2:YAS_total_rows])

# Modify chief complaint value that contains an unquoted comma
YAS_pattern[, V1 := sub("Traumatic Injuries, Specific", "Traumatic Injuries Specific", V1, fixed = TRUE)]

# Remove all quotes
YAS_pattern[, V1 := gsub("\"", "", V1, fixed = TRUE)]

# Field 23 is free text and often contains commas - so we quote field 23
YAS_pattern[, cleaned_record := sub("^((?:[^,]*,){22})(.*)((?:,NHSD|,NULL|,PSIAM)(?:,[^,]*,))(.*)((?:,[^,]*)(?:,0|,1)(?:,[^,]*,))(.*)((?:,[^,]*){4})$", "\\1\"\\2\"\\3\"\\4\"\\5\"\\6\"\\7", V1, perl = TRUE)]

# Now, finally, read data as a data.table
yas_amb_data <- data.table(iotools::dstrsplit(YAS_pattern[, cleaned_record], col_types = rep("character", 34), sep = ",", quote = "\""))

rm(YAS_pattern, YAS_top_line)
gc()

# Set field names
field_names <- c("anonymous_id",
  "call_stopped_reason",
  "time_clock_start",
  "time_of_call",
  "call_type",
  "chief_complaint",
  "urgency_level",
  "interval_allocation_from_time_of_call",
  "interval_response_time_any",
  "urgency_level_time_at_scene",
  "interval_response_time_conveying",
  "time_at_switchboard",
  "time_of_call_2",
  "time_clock_start_2",
  "time_at_switchboard_2",
  "time_call_answered",
  "time_reason_for_call",
  "time_chief_complaint",
  "TimeClinQualClockStart",
  "PerfBestRespClinQual",
  "PerfBestConvRespClinQual",
  "dispatch_code",
  "XXX.dispatch_code_description",
  "OtherAgencyType",
  "MainPatientDiagnosis",
  "age",
  "MethodOfCall",
  "PerfConvRespExists",
  "OtherAgencyNoSendCodePSIAM",
  "OtherAgencyNoSendCodeTEXT",
  "HCP_Emergency",
  "call_source",
  "post_district",
  "lsoa")
setnames(yas_amb_data, field_names)

# Keep only valid calls
valid_call_stop <- c('Back to Bed', 'Clinical Advisor dealing', 'Diabetic Referral', 'ECP Dealing', 'Falls Referral', 'NULL', 'Patient Treated On Scene', 'Referred to GP', 'Required but not conveyed', 'Resolved Through TEL Advice')
yas_amb_data <- yas_amb_data[!is.na(time_of_call) & !is.na(lsoa) & call_stopped_reason %in% valid_call_stop]

# assign outcome
yas_amb_data[, outcome_of_incident := "1"]
yas_amb_data[call_stopped_reason %in% c('Clinical Advisor dealing', 'Resolved Through TEL Advice'), outcome_of_incident := "3"]
yas_amb_data[call_stopped_reason %in% c('Back to Bed', 'Diabetic Referral', 'ECP Dealing', 'Falls Referral', 'Patient Treated On Scene', 'Referred to GP', 'Required but not conveyed'), outcome_of_incident := "5"]


## assign month of call
yas_amb_data[, ':=' (yearmonth = as.Date(lubridate::fast_strptime(paste0(substr(time_at_switchboard, 1, 7), "-01"), format = "%Y-%m-%d", lt = FALSE)),
  call_to_scene_any = as.integer(lubridate::fast_strptime(substr(yas_amb_data$interval_response_time_any, 1, 19), format = "%Y-%m-%d %H:%M:%S", lt = FALSE) - as.POSIXct("1900-01-01 00:00.00 UTC")),
  call_to_scene_conveying = as.integer(NA),
  scene_to_dest = as.integer(NA),
  call_to_dest = as.integer(NA),
  dest_to_clear = as.integer(NA))]

yas_amb_data[call_to_scene_any < 0, call_to_scene_any := 0]

yas_amb_data_red_calls <- yas_amb_data[call_type == "Emergency" & urgency_level %in% c('Purple', 'Red', 'Red1', 'Red2', ' 1 R2 Medical'), .(yearmonth, lsoa, outcome_of_incident, call_to_scene_any, call_to_scene_conveying, scene_to_dest, call_to_dest, dest_to_clear)]
yas_amb_data_green_calls <- yas_amb_data[call_type == "Emergency" & !(urgency_level %in% c('Purple', 'Red', 'Red1', 'Red2', ' 1 R2 Medical')), .(yearmonth, lsoa, outcome_of_incident)]

rm(yas_amb_data)


# Combine datsets - part 2 ------------------------------------------------


amb_data_red_calls_list <- lapply(services[5:8], function(serv) {
  return(get(paste0(serv, "_amb_data_red_calls"))[, ambulance_service := serv])
})
amb_data_red_calls_part2 <- rbindlist(amb_data_red_calls_list)
save(amb_data_red_calls_part2, file = "data/amb_data_red_calls_part2.Rda", compress = "xz")

rm(amb_data_red_calls_list, amb_data_red_calls_part2)
rm(list = paste0(services[5:8], "_amb_data_red_calls"))
gc()



amb_data_green_calls_list <- lapply(services[5:8], function(serv) {
  return(get(paste0(serv, "_amb_data_green_calls"))[, ambulance_service := serv])
})
amb_data_green_calls_part2 <- rbindlist(amb_data_green_calls_list)

rm(amb_data_green_calls_list)
rm(list = paste0(services[5:8], "_amb_data_green_calls"))
gc()


load("data/amb_data_green_calls_part1.Rda")
amb_data_green_calls <- rbindlist(list(amb_data_green_calls_part1, amb_data_green_calls_part2))
save(amb_data_green_calls, file = "data/amb_data_green_calls.Rda", compress = "xz")

rm(amb_data_green_calls_part1, amb_data_green_calls_part2, amb_data_green_calls)
gc()

load("data/amb_data_red_calls_part1.Rda")
load("data/amb_data_red_calls_part2.Rda")
amb_data_red_calls <- rbindlist(list(amb_data_red_calls_part1, amb_data_red_calls_part2))
save(amb_data_red_calls, file = "data/amb_data_red_calls.Rda", compress = "xz")

rm(amb_data_red_calls_part1, amb_data_red_calls_part2, amb_data_red_calls)
gc()

file.remove(c("data/amb_data_green_calls_part1.Rda", "data/amb_data_red_calls_part1.Rda", "data/amb_data_red_calls_part2.Rda"))

