library(data.table)


generateDfTCatchmentAreaSets <- function() {
  load("data/dft travel times.Rda")

  # For each lsoa for each year, rank the nearest destination by travel time
  travel_time_data_ranked <- travel_time_data[, .(destination, N = travel_time, destination_rank = rank(travel_time, ties.method = "random")), by = .(lsoa, year)]

  # Keep only the closest (by time) two destinations for each (lsoa, year)-tuple & only keep the second if it is to a connected destination
  travel_time_data_selected <- travel_time_data_ranked[destination_rank == 1 | (destination_rank == 2 & !is.na(destination)), ]


  ################################################################################
  # Identify lsoas we need to attach a destination to (due to not being connected to road network in DfT data)
  # not_connected_lsoas <- travel_time_data_selected[is.na(destination), .(lsoa, year = paste0("y", year), is_not_connected = 1)]
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
  travel_time_data_selected[year == 2009 & lsoa == "E01020292", destination := "north devon district"]
  travel_time_data_selected[year == 2011 & lsoa == "E01031246", destination := "warwick"]
  travel_time_data_selected[year == 2011 & lsoa == "E01031268", destination := "warwick"]
  travel_time_data_selected[year == 2011 & lsoa == "E01019313", destination := "cumberland infirmary"]

  # Tidy up
  travel_time_data_selected[, ':=' (method = paste0("DfT", year),
    year = NULL)]

  return(travel_time_data_selected)
}

dft_catchment_area_sets <- generateDfTCatchmentAreaSets()




# name appropriately and reshape data
dft_catchment_areas <- destination_least_travel_time[tied == FALSE, .(lsoa, destination, year = paste0("y", year, "_nearest_ae"))]
dft_catchment_areas <- dcast(dft_catchment_areas, lsoa ~ year, fill = NA, value.var = "destination")

# Save catchments areas files
save(dft_catchment_areas, file = "data/dft catchment areas.Rda")
