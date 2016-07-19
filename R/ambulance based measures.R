#################################################
# This needs re-writing such that each service's data is loaded only once


ambulance_times_lsoa <- function(amb_times_data, service) {

  # reshape amb_times_data
  amb_times_data_long <- data.table::melt(amb_times_data, id.vars = c("yearmonth", "lsoa"),
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
  amb_times_data_long <- data.table::melt(amb_times_data, id.vars = c("yearmonth", "lsoa"),
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

  #   load("data/site data.Rda")
  #   services <- tolower(unique(site_data$ambulance_service))

  services <- c("emas", "neas")

  service_data_list <- lapply(services, function(service) {
    amb_data <- load(paste0("data/amb_red_calls_", service, "_data.Rda"))
    lsoa_level_measure_data <- ambulance_times_lsoa(get(amb_data), service)

    site_level_measure_data <- ambulance_times_site(get(amb_data), service)
    site_level_measure_data[, ':=' (diff_time_to_ed = as.integer(NA),
      lsoa = as.character(NA))]

    setcolorder(site_level_measure_data, colnames(lsoa_level_measure_data))

    return(rbindlist(list(lsoa_level_measure_data, site_level_measure_data)))
  })

  # bind lists
  amb_mean_times <- rbindlist(service_data_list)

  amb_mean_times_measure <- amb_mean_times[!is.na(lsoa)]
  amb_mean_times_site_measure <- amb_mean_times[is.na(lsoa)]

  amb_mean_times_site_measure[, c("diff_time_to_ed", "lsoa") := NULL]

  rm(amb_mean_times)
  gc()

  # save
  save(amb_mean_times_measure, file = createMeasureFilename("ambulance mean times"), compress = "bzip2")
  save(amb_mean_times_site_measure, file = createMeasureFilename("ambulance mean times", "site"))
}




save_ambulance_red_calls_measure <- function() {

  #   load("data/site data.Rda")
  #   services <- tolower(unique(site_data$ambulance_service))

  services <- c("emas", "neas")

  service_data_list <- lapply(services, function(service) {
    amb_data <- load(paste0("data/amb_red_calls_", service, "_data.Rda"))
    lsoa_level_measure_data <- ambulance_call_vols_lsoa(get(amb_data), service)

    site_level_measure_data <- collapseLsoas2Sites(lsoa_level_measure_data)
    site_level_measure_data[, ':=' (diff_time_to_ed = as.integer(NA),
      lsoa = as.character(NA))]

    return(rbind(lsoa_level_measure_data, site_level_measure_data))
  })

  # bind lists
  amb_red_calls <- rbindlist(service_data_list)

  amb_red_calls_measure <- amb_red_calls[!is.na(lsoa)]
  amb_red_calls_site_measure <- amb_red_calls[is.na(lsoa)]

  amb_red_calls_site_measure[, c("diff_time_to_ed", "lsoa") := NULL]

  rm(amb_red_calls)
  gc()

  # save
  save(amb_red_calls_measure, file = createMeasureFilename("ambulance red calls"), compress = "bzip2")
  save(amb_red_calls_site_measure, file = createMeasureFilename("ambulance red calls", "site"))
}


ambulance_call_vols_lsoa <- function(amb_times_data, service) {
  total_calls <- amb_times_data[, .(value = .N), by = .(yearmonth, lsoa)]
  total_calls[, sub_measure := "total"]

  hosp_transfer_calls <- amb_times_data[outcome_of_incident == "2", .(value = .N), by = .(yearmonth, lsoa)]
  hosp_transfer_calls[, sub_measure := "hospital transfers"]

  # combine
  ambulance_call_vols <- rbind(total_calls, hosp_transfer_calls)
  # add measure var
  ambulance_call_vols[, measure := "ambulance red calls"]

  # format
  ambulance_call_vols_filled <- fillDataPoints(ambulance_call_vols, TRUE, TRUE)

  # attach service name
  ambulance_call_vols_filled_service <- attachAmbulanceService(ambulance_call_vols_filled)

  # remove data for areas outwith service
  ambulance_call_vols_measure <- ambulance_call_vols_filled_service[tolower(ambulance_service) == service]

  ambulance_call_vols_measure[, ambulance_service := NULL]

  return(ambulance_call_vols_measure)
}


save_ambulance_non_conveyance_measure <- function() {

  #   load("data/site data.Rda")
  #   services <- tolower(unique(site_data$ambulance_service))

  services <- c("emas", "neas")

  service_data_list <- lapply(services, function(service) {
    amb_data <- load(paste0("data/amb_green_calls_", service, "_data.Rda"))
    lsoa_level_measure_data <- ambulance_non_conveyance_lsoa(get(amb_data), service)

    site_level_measure_data_no_frac <- collapseLsoas2Sites(lsoa_level_measure_data[sub_measure != "fraction not conveyed"])
    site_level_measure_data <- addFractionSubmeasure(site_level_measure_data_no_frac, "green calls", "not conveyed green calls", "fraction not conveyed")
    site_level_measure_data[, ':=' (diff_time_to_ed = as.integer(NA),
        lsoa = as.character(NA))]

    return(rbind(lsoa_level_measure_data, site_level_measure_data))
  })

  # bind lists
  amb_green_calls <- rbindlist(service_data_list)

  amb_non_conveyance_measure <- amb_green_calls[!is.na(lsoa)]
  amb_non_conveyance_site_measure <- amb_green_calls[is.na(lsoa)]

  amb_non_conveyance_site_measure[, c("diff_time_to_ed", "lsoa") := NULL]

  rm(amb_green_calls)
  gc()

  # save
  save(amb_non_conveyance_measure, file = createMeasureFilename("ambulance non-conveyance"), compress = "bzip2")
  save(amb_non_conveyance_site_measure, file = createMeasureFilename("ambulance non-conveyance", "site"))
}


ambulance_non_conveyance_lsoa <- function(amb_times_data, service) {
  total_calls <- amb_times_data[, .(value = .N), by = .(yearmonth, lsoa)]
  total_calls[, sub_measure := "green calls"]

  hosp_transfer_calls <- amb_times_data[outcome_of_incident == "3" | outcome_of_incident == "5", .(value = .N), by = .(yearmonth, lsoa)]
  hosp_transfer_calls[, sub_measure := "not conveyed green calls"]

  # combine
  ambulance_call_vols <- rbind(total_calls, hosp_transfer_calls)
  # add measure var
  ambulance_call_vols[, measure := "ambulance non-conveyance"]

  # format
  ambulance_call_vols_filled <- fillDataPoints(ambulance_call_vols, TRUE, TRUE)

  # add fraction
  ambulance_call_vols_filled_frac <- addFractionSubmeasure(ambulance_call_vols_filled, "green calls", "not conveyed green calls", "fraction not conveyed")

  # attach service name
  ambulance_call_vols_filled_service <- attachAmbulanceService(ambulance_call_vols_filled_frac)

  # remove data for areas outwith service
  ambulance_call_vols_measure <- ambulance_call_vols_filled_service[tolower(ambulance_service) == service]

  ambulance_call_vols_measure[, ambulance_service := NULL]

  return(ambulance_call_vols_measure)
}


# # Test code
# bla <- copy(amb_mean_times_site_measure)
# bla[, missing := is.na(value)]
# bla[, .N, by = .(town, sub_measure, missing)]
#
# op1 <- bla[sub_measure == "call_to_dest", .N, by = .(town, missing)]
# setorderv(op1, c("town", "missing"))
# write.table(op1, file = "clipboard", sep = "\t", row.names = FALSE)
