library(data.table)

load("data/dft travel times.Rda")

# For each LSOA for each year, rank the nearest destination by travel time
travel_time_data_ranked <- travel_time_data[, .(destination, travel_time, destination_rank = rank(travel_time, ties.method = "min")), by = .(LSOA, year)]

# This remove the cases where there is are multiple unconnected detsinations (i.e. destination = NA and travel_time = 10000000) for each (LSOA, year)-tuple
setkey(travel_time_data_ranked, NULL)
travel_time_data_ranked <- unique(travel_time_data_ranked)

# Keep only the destination with the top rank for each (LSOA, year)-tuple
destination_least_travel_time <- travel_time_data_ranked[destination_rank == 1, .(sum_ranks = sum(destination_rank), destination), by = .(LSOA, year)]
destination_least_travel_time[sum_ranks > 1, tied := TRUE]
destination_least_travel_time[sum_ranks == 1 & !is.na(destination), tied := FALSE]
destination_least_travel_time[is.na(destination), tied := NA]
destination_least_travel_time[, sum_ranks := NULL]


################################################################################
# Identify LSOAs we need to attach a destination to (due to not being connected to road network in DfT data)
# # Find LSOAs with no nearest destination - convert to wide to see the year(s) when each is not connected
# not_connected_LSOAs <- destination_least_travel_time[is.na(destination), .(LSOA, year = paste0("y", year), is_not_connected = 1)]
# not_connected_LSOAs <- dcast(not_connected_LSOAs, LSOA ~ year, fill = 0, value.var = "is_not_connected")
# not_connected_cats <- c("2009 only", "2011 only", "2009 and 2011")
# not_connected_LSOAs[, not_connected_cat := not_connected_cats[y2009 + 2 * y2011]]
# not_connected_LSOAs[, c("y2009", "y2011") := NULL]
#
# # Load LSOAs for which we have HES data for the residents
# load("data/lsoa 2001s in hes data.Rda")
#
# # Identify which of our unconnected LSOAs lie in this area
# LSOAs <- not_connected_LSOAs$LSOA
# relevant_LSOAs <- LSOAs[LSOAs %in% lsoas_in_hes_data]
#
# # Read in population weighted centroids (PWCs) for the LSOAs and keep only the relavant PWCs
# LSOA_pwcs <- readShapePoints("data-raw/geography data/Centroids/LSOA_2001_EW_PWC.shp")
# LSOA_pwcs_selected <- LSOA_pwcs[LSOA_pwcs$LSOA01CD %in% relevant_LSOAs, ]
# proj4string(LSOA_pwcs_selected) <- "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +towgs84=446.448,-125.157,542.06,0.15,0.247,0.842,-20.489 +units=m +vunits=m +no_defs"
#
# # Merge in data regarding for what years these LSOAs were not connected
# LSOA_pwcs_selected <- merge(LSOA_pwcs_selected, unconnected.LSOAs, by.x = "LSOA01CD", by.y = "LSOA")
#
# # Write to shp file
# writePointsShape(LSOA_pwcs_selected, "data/not connected relevant LSOA centroids")
#
# Visually compare with catchment areas
################################################################################

# Manual changes to (only relevant) catchment areas
destination_least_travel_time[year == 2009 & LSOA == "E01020292", ':=' (destination = "north devon district",
  tied = FALSE)]
destination_least_travel_time[year == 2011 & LSOA == "E01031246", ':=' (destination = "warwick",
  tied = FALSE)]
destination_least_travel_time[year == 2011 & LSOA == "E01031268", ':=' (destination = "warwick",
  tied = FALSE)]
destination_least_travel_time[year == 2011 & LSOA == "E01019313", ':=' (destination = "cumberland infirmary",
  tied = FALSE)]

# name appropriately and reshape data
dft_catchment_areas <- destination_least_travel_time[tied == FALSE, .(LSOA, destination, year = paste0("y", year, "_nearest_ae"))]
dft_catchment_areas <- dcast(dft_catchment_areas, LSOA ~ year, fill = NA, value.var = "destination")

# Save catchments areas files
save(dft_catchment_areas, file = "data/dft catchment areas.Rda")
