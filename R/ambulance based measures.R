ambulance_times_lsoa <- function(amb_times_data, service) {

  # reshape amb_times_data
  amb_times_data_long <- melt(amb_times_data, id.vars = c("yearmonth", "lsoa"),
    measure.vars = c("call_to_scene_any", "call_to_scene_conveying", "scene_to_dest", "call_to_dest", "dest_to_clear"),
    variable.name = "sub_measure", variable.factor = FALSE)

  # collapse to means by (lsoa, month, sub_measure)
  amb_times_lsoa_month <- amb_times_data_long[, .(value = round(mean(as.integer(value), na.rm = TRUE))), by = .(lsoa, yearmonth, sub_measure)]

  # add measure var
  amb_times_lsoa_month[, measure := "ambulance mean times"]

  # format
  amb_times_lsoa_month_filled <- fillDataPoints(amb_times_lsoa_month, FALSE, TRUE)

  # attach service name
  amb_times_lsoa_month_filled_service <- attachAmbulanceService(amb_times_lsoa_month_filled)

  # remove data for areas outwith service
  amb_mean_times_measure <- amb_times_lsoa_month_filled_service[tolower(ambulance_service) == service]

  # remove service var
  amb_mean_times_measure[, ambulance_service := NULL]

  return(amb_mean_times_measure)
}



ambulance_times_site <- function(amb_times_data, service) {

  # reshape amb_times_data
  amb_times_data_long <- melt(amb_times_data, id.vars = c("yearmonth", "lsoa"),
    measure.vars = c("call_to_scene_any", "call_to_scene_conveying", "scene_to_dest", "call_to_dest", "dest_to_clear"),
    variable.name = "sub_measure", variable.factor = FALSE)

  # collapse to means by (town, month, sub_measure)
  amb_times_lsoa_town_month <- attachTownForSiteLevelData(amb_times_data_long)
  amb_times_town_month <- amb_times_lsoa_town_month[, .(value = round(mean(as.integer(value), na.rm = TRUE))), by = .(town, yearmonth, sub_measure)]

  amb_times_town_month[, measure := "ambulance mean times"]

  # format
  amb_times_town_month_filled <- fillDataPoints(amb_times_town_month, FALSE, FALSE)

  # attach service name
  amb_times_town_month_filled_service <- attachAmbulanceService(amb_times_town_month_filled)

  # remove data for areas outwith service
  amb_mean_times_site_measure <- amb_times_town_month_filled_service[tolower(ambulance_service) == service]

  amb_mean_times_site_measure[, ambulance_service := NULL]

  return(amb_mean_times_site_measure)
}


save_ambulance_timings_measure <- function() {

  ## re-write so works for ALL services
#   load("data/site data.Rda")
#   services <- tolower(unique(site_data$ambulance_service))

  services <- c("emas", "neas")
  paths <- paste0("data/", services, "_amb_timings_data.Rda")

  load(paths[1])
  load(paths[2])

  amb_mean_times_measure_list <- list(ambulance_times_lsoa(emas_amb_data_red_calls, "emas"),
    ambulance_times_lsoa(neas_amb_data_red_calls, "neas"))

  amb_mean_times_site_measure_list <- list(ambulance_times_site(emas_amb_data_red_calls, "emas"),
    ambulance_times_site(neas_amb_data_red_calls, "neas"))

  # bind lists
  amb_mean_times_measure <- rbindlist(amb_mean_times_measure_list)
  amb_mean_times_site_measure <- rbindlist(amb_mean_times_site_measure_list)

  # save
  save(amb_mean_times_measure, file = createMeasureFilename("ambulance mean times"), compress = "bzip2")
  save(amb_mean_times_site_measure, file = createMeasureFilename("ambulance mean times", "site"))
}
