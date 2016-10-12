

save_ambulance_red_calls_measures <- function() {

  load("data/site data.Rda")
  load("D:/Rpackages/rclosed/data/catchment area set final.Rda")
  ambulance_data_valid_lsoas <- merge(site_data[, .(town, ambulance_service = tolower(ambulance_service))], catchment_area_set_final[, .(town, lsoa)], by = "town")

  load("data/amb_data_red_calls.Rda")

  amb_data_red_calls_in_ca <- merge(amb_data_red_calls, ambulance_data_valid_lsoas, by = c("lsoa", "ambulance_service"))[, ambulance_service := NULL]


  # Times based measures ------------------------------------------------

  amb_mean_times <- calc_ambulance_times_measures(amb_data_red_calls_in_ca, FALSE, TRUE)
  amb_mean_times_site <- calc_ambulance_times_measures(amb_data_red_calls_in_ca, FALSE, FALSE)

  amb_mean_times_n <- calc_ambulance_times_measures(amb_data_red_calls_in_ca, TRUE, TRUE)[, c("measure", "group", "site_type", "relative_month", "diff_time_to_ed") := NULL]
  amb_mean_times_site_n <- calc_ambulance_times_measures(amb_data_red_calls_in_ca, TRUE, FALSE)[, c("measure", "group", "site_type", "relative_month") := NULL]

  setnames(amb_mean_times_n, "value", "n")
  setnames(amb_mean_times_site_n, "value", "n")

  amb_mean_times_measure <- merge(amb_mean_times, amb_mean_times_n, by = c("lsoa", "town", "yearmonth", "sub_measure"), all = TRUE)
  amb_mean_times_site_measure <- merge(amb_mean_times_site, amb_mean_times_site_n, by = c("town", "yearmonth", "sub_measure"), all = TRUE)

  # save
  save(amb_mean_times_measure, file = createMeasureFilename("ambulance mean times"), compress = "bzip2")
  save(amb_mean_times_site_measure, file = createMeasureFilename("ambulance mean times", "site"))


  # Call volume based measures ------------------------------------------------

  amb_red_calls_measure <- calc_ambulance_vol_measures(amb_data_red_calls_in_ca, TRUE)
  amb_red_calls_site_measure <- calc_ambulance_vol_measures(amb_data_red_calls_in_ca, FALSE)

  # save
  save(amb_red_calls_measure, file = createMeasureFilename("ambulance red calls"), compress = "bzip2")
  save(amb_red_calls_site_measure, file = createMeasureFilename("ambulance red calls", "site"))

}



calc_ambulance_vol_measures <- function(amb_calls_data, lsoa_level = TRUE) {

  if(lsoa_level) {
    amb_vols_geo_month <- amb_calls_data[, .(value = .N), by = .(lsoa, yearmonth)]
    ht_vols_geo_month <- amb_calls_data[outcome_of_incident == "2", .(value = .N), by = .(lsoa, yearmonth)]
  } else {
    amb_vols_geo_month <- amb_calls_data[, .(value = .N), by = .(town, yearmonth)]
    ht_vols_geo_month <- amb_calls_data[outcome_of_incident == "2", .(value = .N), by = .(town, yearmonth)]
  }

  amb_vols_geo_month[, sub_measure := "total"]
  ht_vols_geo_month[, sub_measure := "hospital transfers"]

  amb_call_geo_month <- rbind(amb_vols_geo_month, ht_vols_geo_month)

  # add measure var
  amb_call_geo_month[, measure := "ambulance red calls"]

  # format
  amb_calls_measure <- fillDataPoints(amb_call_geo_month, TRUE, lsoa_level)

  return(amb_calls_measure)
}





calc_ambulance_times_measures <- function(amb_times_data, counts = FALSE, lsoa_level = TRUE) {

  # reshape amb_times_data
  amb_times_data_long <- melt(amb_times_data, id.vars = c("yearmonth", "lsoa", "town"),
    measure.vars = c("call_to_scene_any", "call_to_scene_conveying", "scene_to_dest", "call_to_dest", "dest_to_clear"),
    variable.name = "sub_measure", variable.factor = FALSE)

  if(counts) {

    if(lsoa_level) {
      amb_times_geo_month <- amb_times_data_long[!is.na(value), .(value = .N), by = .(lsoa, yearmonth, sub_measure)]
    } else {
      amb_times_geo_month <- amb_times_data_long[!is.na(value), .(value = .N), by = .(town, yearmonth, sub_measure)]
    }

    # add measure var
    amb_times_geo_month[, measure := "ambulance mean times"]

  } else {

    if(lsoa_level) {
      amb_times_geo_month <- amb_times_data_long[, .(value = round(mean(as.integer(value), na.rm = TRUE))), by = .(lsoa, yearmonth, sub_measure)]
    } else {
      amb_times_geo_month <- amb_times_data_long[, .(value = round(mean(as.integer(value), na.rm = TRUE))), by = .(town, yearmonth, sub_measure)]
    }

    # add measure var
    amb_times_geo_month[, measure := "ambulance mean times n"]
  }

  # format
  amb_times_measure <- fillDataPoints(amb_times_geo_month, counts, lsoa_level)

  return(amb_times_measure)
}





save_ambulance_green_calls_measures <- function() {

  load("data/site data.Rda")
  load("D:/Rpackages/rclosed/data/catchment area set final.Rda")
  ambulance_data_valid_lsoas <- merge(site_data[, .(town, ambulance_service = tolower(ambulance_service))], catchment_area_set_final[, .(town, lsoa)], by = "town")

  load("data/amb_data_green_calls.Rda")

  amb_data_green_calls_in_ca <- merge(amb_data_green_calls, ambulance_data_valid_lsoas, by = c("lsoa", "ambulance_service"))[, ambulance_service := NULL]


  # Non-conveyance measures -------------------------------------------------

  amb_non_conveyance_measure <- calc_non_conveyance_measure(amb_data_green_calls_in_ca, TRUE)
  amb_non_conveyance_site_measure <- calc_non_conveyance_measure(amb_data_green_calls_in_ca, FALSE)

  # save
  save(amb_non_conveyance_measure, file = createMeasureFilename("ambulance non-conveyance"), compress = "bzip2")
  save(amb_non_conveyance_site_measure, file = createMeasureFilename("ambulance non-conveyance", "site"))
}




calc_non_conveyance_measure <- function(amb_calls_data, lsoa_level = TRUE) {

  if(lsoa_level) {
    amb_vols_geo_month <- amb_calls_data[, .(value = .N), by = .(lsoa, yearmonth)]
    nc_vols_geo_month <- amb_calls_data[outcome_of_incident == "3" | outcome_of_incident == "5", .(value = .N), by = .(lsoa, yearmonth)]
  } else {
    amb_vols_geo_month <- amb_calls_data[, .(value = .N), by = .(town, yearmonth)]
    nc_vols_geo_month <- amb_calls_data[outcome_of_incident == "3" | outcome_of_incident == "5", .(value = .N), by = .(town, yearmonth)]
  }

  amb_vols_geo_month[, sub_measure := "green calls"]
  nc_vols_geo_month[, sub_measure := "not conveyed green calls"]

  amb_call_geo_month <- rbind(amb_vols_geo_month, nc_vols_geo_month)

  # add measure var
  amb_call_geo_month[, measure := "ambulance non-conveyance"]

  # format
  amb_call_geo_month_formatted <- fillDataPoints(amb_call_geo_month, TRUE, lsoa_level)

  # add fraction
  amb_nonconveyed_measure <- addFractionSubmeasure(amb_call_geo_month_formatted, "green calls", "not conveyed green calls", "fraction not conveyed")

  return(amb_nonconveyed_measure)
}
