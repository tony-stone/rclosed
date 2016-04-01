library(data.table)

# Generic fn for First Past the Post for HES and Amb data -----------------

genFPPAreaSets <- function(end_date_exc, data_src, period_length_months = 12) {
  # Function to generate First Past the Post catchment areas
  # data_src              - 'volumes' by 'month', 'destination', 'lsoa' (field names, as character type, month as 'end_date_exc', below) [data.table type]
  # end_date_exc          - year and month of the first month (after start.dat.inc) that we wish to exclude [Date type]
  # period_length_months  - how long (months) before end_date_exc to look [integer; defaults to 12]

  # Calculate start date based on period
  start_date_inc <- end_date_exc
  lubridate::month(start_date_inc) <- lubridate::month(start_date_inc) - period_length_months

  # Ambulance data does not always go back sufficiently far
  if(end_date_exc <= min(data_src$yearmonth)) return(NA)

  # Keep only rows within the time period of interest and sum the volumes by distinct (destination, lsoa)-pairs over the time period of interest
  volume_data_summed_over_period <- data_src[yearmonth >= start_date_inc & yearmonth < end_date_exc, .(N = sum(volume)), by=c("destination", "lsoa")]

  # For each lsoa, rank volume (number of attendances/admissions/calls) - if tied, ranks are allocated randomly
  volume_data_summed_over_period[, destination_rank := rank(N, ties.method = "random"), by = lsoa]

  # Create inverted rank/rownum and total volume fields
  volume_data_summed_over_period[, ':=' ( destination_rank = max(destination_rank) - destination_rank + 1L,
    N_denominator = as.integer(sum(N))),
    by = lsoa]
  ## Keep only first and second ranked destinations for each LSOA, subject to following rules applied in the specified order:
  top2_destinations_by_lsoa <- volume_data_summed_over_period[destination_rank == 1 | destination_rank == 2, ]

  # Attach intervention_date and period_length
  top2_destinations_by_lsoa[, ':=' (ref_date = end_date_exc,
    period_length = period_length_months)]

  return(top2_destinations_by_lsoa)
}




# reshape the data --------------------------------------------------------

reshapeData <- function(data) {

  # Convert to wide form (this handles LSOAs with no second destination) and identify ties
  top2_destinations_by_lsoa <- dcast(data, lsoa + N_denominator + data_source + ref_date + period_length ~ destination_rank, value.var = c("N", "destination", "unique_code"), fill = 0L)
  top2_destinations_by_lsoa[, tied := FALSE]
  top2_destinations_by_lsoa[N_1 == N_2, ':=' (tied = TRUE,
    destination_1 = NA,
    destination_2 = NA,
    unique_code_1 = NA,
    unique_code_2 = NA
    )]
  top2_destinations_by_lsoa[N_2 == 0, ':=' (destination_2 = NA,
    unique_code_2 = NA)]


  # Calc fraction of attendances at any destination from this LSOA, to the specific destination; diff between volume between first and second destination (as fraction); and remove unnecessary fields
  top2_destinations_by_lsoa[, ':=' (frac_to_destination = N_1 / N_denominator,
    diff_first_second = abs(N_1 - N_2),
    N_2 = NULL)][, ':=' (diff_first_second_frac = diff_first_second / N_denominator,
      frac_to_destination_grp = cut(frac_to_destination, breaks = 0:10/10, include.lowest = TRUE, right = FALSE))]

  # Attach Trust details
  load("data/site data.Rda")
  top2_destinations_by_lsoa <- merge(top2_destinations_by_lsoa, site_data, by.x = "unique_code_1", by.y = "unique_code", all.x = TRUE)
  top2_destinations_by_lsoa <- merge(top2_destinations_by_lsoa, site_data[, .(unique_code, town)], by.x = "unique_code_2", by.y = "unique_code", all.x = TRUE)

  # Sort names
  var_names <- c("N", "unique_code", "destination")
  setnames(top2_destinations_by_lsoa, c(paste0(var_names, "_1"), "town.x", "town.y"), c(var_names, "town", "town_2"))

  return(top2_destinations_by_lsoa)
}





# HES A&E catchment areas -------------------------------------------------

generateHESCatchmentAreaSets <- function() {
  # read
  load("data/site data.Rda")

  # get periods for which to calc catchment areas
  intervention_dates <- unique(site_data[, intervention_date])

  # Load data
  load("data/attendances by trust lsoa month.Rda")

  # attds by 3 char procode/month/lsoa
  attendances_by_month_destination_lsoa <- attendances_by_trust_lsoa_month[, .(volume = sum(attendances)), by= .(procode3, lsoa, yearmonth)]
  setnames(attendances_by_month_destination_lsoa, "procode3", "destination")

  # Free up memory, remove unnecessary vars
  rm(attendances_by_trust_lsoa_month)
  gc()

  # Create catchment areas
  catchment_area_sets_list <- list()
  for(period_length in seq(6, 24, 6)) {
    catchment_area_sets_list <- c(catchment_area_sets_list, lapply(intervention_dates, genFPPAreaSets, data_src = attendances_by_month_destination_lsoa, period_length_months = period_length))
  }

  # Bind data together
  hes_ae_catchment_areas <- rbindlist(catchment_area_sets_list)

  # Add source field
  hes_ae_catchment_areas[, data_source := "HES A&E"]

  # Attach intervention/control details
  load("data/site data.Rda")
  hes_ae_catchment_areas <- merge(hes_ae_catchment_areas, site_data[, .(trust_code, intervention_date, unique_code)], by.x = c("destination", "ref_date"), by.y = c("trust_code", "intervention_date"), all.x = TRUE)

  # Convert to wide format
  hes_ae_catchment_areas <- reshapeData(hes_ae_catchment_areas)

  # Convert from procode to trust name
  load("data/trust names data.Rda")
  hes_ae_catchment_areas <- merge(hes_ae_catchment_areas, trust_names_data[, .(trust_code, trust_name)], by.x = "destination", by.y = "trust_code", all.x = TRUE)
  hes_ae_catchment_areas <- merge(hes_ae_catchment_areas, trust_names_data[, .(trust_code, trust_name)], by.x = "destination_2", by.y = "trust_code", all.x = TRUE)

  # ODS records miss one code (NM1) - found from HSCIC report (closed March 2013)
  hes_ae_catchment_areas[destination == "NM1", trust_name.x := "WEST LANCASHIRE HEALTHCARE PARTNERSHIP COMMUNITY CIC"]
  hes_ae_catchment_areas[destination_2 == "NM1", trust_name.y := "WEST LANCASHIRE HEALTHCARE PARTNERSHIP COMMUNITY CIC"]

  # Make replacement
  hes_ae_catchment_areas[, ':=' (destination = trust_name.x,
    destination_2 = trust_name.y,
    trust_name.x = NULL,
    trust_name.y = NULL)]

  return(hes_ae_catchment_areas)
}



# Ambulance service catchment areas ---------------------------------------


generateAmbulanceDataCatchmentAreaSets <- function() {

  load("data/site data.Rda")

  # get periods for which to calc catchment areas
  intervention_dates <- unique(site_data[ambulance_service == "EMAS", intervention_date])

  ## Load data
  # EMAS
  load("data/emas conveyances by site lsoa month.Rda")
  # etc
  # etc

  # bind together service data
  conveyances_by_site_lsoa_month <- rbindlist(list(emas_conveyances_by_site_lsoa_month))


  # Create catchment areas
  catchment_area_sets_list <- list()
  amb_service <- "EMAS"
#Add amb service for loop??? or filter on merge later?
  for(period_length in seq(6, 24, 6)) {
    catchment_area_sets_list <- c(catchment_area_sets_list, lapply(intervention_dates, genFPPAreaSets, data_src = conveyances_by_site_lsoa_month[service == amb_service, .(yearmonth, destination, lsoa, volume)], period_length_months = period_length))
  }

  # Bind data together
  ambulance_data_catchment_areas <- rbindlist(catchment_area_sets_list[!is.na(catchment_area_sets_list)])

  # Add source field
  ambulance_data_catchment_areas[, data_source := "Ambulance Data"]

  # Attach intervention/control details
  ambulance_data_catchment_areas <- merge(ambulance_data_catchment_areas, site_data[, .(dft_name, intervention_date, unique_code)], by.x = c("destination", "ref_date"), by.y = c("dft_name", "intervention_date"), all.x = TRUE)

  # Convert to wide format
  ambulance_data_catchment_areas <- reshapeData(ambulance_data_catchment_areas)

  return(ambulance_data_catchment_areas)
}



# DfT catchment areas -----------------------------------------------------

generateDfTCatchmentAreaSets <- function() {
  load("data/dft travel times.Rda")

  # For each lsoa for each year, rank the nearest destination by travel time
  suppressWarnings(travel_time_data_ranked <- travel_time_data[, .(destination, N = travel_time, N_denominator = as.double(min(travel_time, na.rm = TRUE)), destination_rank = rank(travel_time, ties.method = "random")), by = .(lsoa, year)])

  # Fix issue with min being set to +Inf if no times
  travel_time_data_ranked[is.infinite(N_denominator), N_denominator := NA]

  # Keep only the closest (by time) two destinations for each (lsoa, year)-tuple & only keep the second if it is to a connected destination
  dft_catchment_areas <- travel_time_data_ranked[destination_rank == 1 | (destination_rank == 2 & !is.na(destination)), ]


  ################################################################################
  # Identify lsoas we need to attach a destination to (due to not being connected to road network in DfT data)
  # not_connected_lsoas <- dft_catchment_areas[is.na(destination), .(lsoa, year = paste0("y", year), is_not_connected = 1)]
  # not_connected_lsoas <- dcast(not_connected_lsoas, lsoa ~ year, fill = 0, value.var = "is_not_connected")
  # not_connected_cats <- c("2009 only", "2011 only", "2009 and 2011")
  # not_connected_lsoas[, not_connected_cat := not_connected_cats[y2009 + 2 * y2011]]
  # not_connected_lsoas[, c("y2009", "y2011") := NULL]
  #
  # # Load lsoas for which we have HES data for the residents
  # load("data/lsoa 2001s in hes data.Rda")
  #
  # # Identify which of our unconnected lsoas lie in this area
  # lsoas <- not_connected_lsoas$lsoa
  # relevant_lsoas <- lsoas[lsoas %in% lsoas_in_hes_data]
  #
  # # Read in population weighted centroids (PWCs) for the lsoas and keep only the relavant PWCs
  # lsoa_pwcs <- readShapePoints("data-raw/geography data/Centroids/lsoa_2001_EW_PWC.shp")
  # lsoa_pwcs_selected <- lsoa_pwcs[lsoa_pwcs$lsoa01CD %in% relevant_lsoas, ]
  # proj4string(lsoa_pwcs_selected) <- "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +towgs84=446.448,-125.157,542.06,0.15,0.247,0.842,-20.489 +units=m +vunits=m +no_defs"
  #
  # # Merge in data regarding for what years these lsoas were not connected
  # lsoa_pwcs_selected <- merge(lsoa_pwcs_selected, unconnected.lsoas, by.x = "lsoa01CD", by.y = "lsoa")
  #
  # # Write to shp file
  # writePointsShape(lsoa_pwcs_selected, "data/not connected relevant lsoa centroids")
  #
  # Visually compare with catchment areas
  ################################################################################


  # Manual changes to (only relevant) catchment areas
  dft_catchment_areas[year == 2009 & lsoa == "E01020292", destination := "north devon district"]
  dft_catchment_areas[year == 2011 & lsoa == "E01031246", destination := "warwick"]
  dft_catchment_areas[year == 2011 & lsoa == "E01031268", destination := "warwick"]
  dft_catchment_areas[year == 2011 & lsoa == "E01019313", destination := "cumberland infirmary"]

  # Keep only the LSOAs for which we have data (some do not due to DfT data source)
  dft_catchment_areas <- dft_catchment_areas[!is.na(destination), ]

  # Add data source attribution and method
  dft_catchment_areas[, ':=' (data_source = "DfT",
    ref_date = as.Date(paste0(year, "-01-01")),
    period_length = as.integer(NA),
    year = NULL)]

  # Attach names for intervention sites
  load("data/site data.Rda")
  dft_catchment_areas <- merge(dft_catchment_areas, site_data[, .(dft_name, dft_year, unique_code)], by.x = c("destination", "ref_date"), by.y = c("dft_name", "dft_year"), all.x = TRUE)

  # Convert to wide format
  dft_catchment_areas <- reshapeData(dft_catchment_areas)

  return(dft_catchment_areas)
}





# main --------------------------------------------------------------------

createCatchmentAreas <- function() {
  dft_catchment_area_sets <- generateDfTCatchmentAreaSets()
  hes_catchment_area_sets <- generateHESCatchmentAreaSets()
  ambulance_data_catchment_area_sets <- generateAmbulanceDataCatchmentAreaSets()

  # All catchment areas
  catchment_area_sets <- rbind(dft_catchment_area_sets, hes_catchment_area_sets, ambulance_data_catchment_area_sets)
  save(catchment_area_sets, file = "data/catchment area sets.Rda", compress = "xz")

  # Project group decided to use DfT catchment areas
  catchment_area_set_final <- catchment_area_sets[data_source == "DfT" & !is.na(unique_code), .(lsoa, unique_code, time_to_destination = N, dose = diff_first_second, is_intervention)]
  catchment_area_set_final[is_intervention == FALSE, dose := 0]
  catchment_area_set_final[, is_intervention := NULL]

  save(catchment_area_set_final, file = "data/catchment area set final.Rda", compress = "bzip2")
}



# execute! ----------------------------------------------------------------
createCatchmentAreas()
