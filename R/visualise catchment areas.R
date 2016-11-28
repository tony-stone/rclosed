library(data.table)
library(leaflet)
library(rgdal)
library(maptools)

# load catchment areas
load("data/catchment area set final.Rda")

# load spatial data
## LSOA boundaries
load("data/lsoa boundary data.Rda")
## site locations
load("data/british ed sites spatial.Rda")

# create catchment area boundaries
catchment_areas <- merge(lsoa_boundary_data, catchment_area_set_final, by.x = "LSOA01CD", by.y = "lsoa", all.x = FALSE, duplicateGeoms = TRUE)

# Union of LSOAs into catchment areas, returns SP (not SPDF)
catchment_area_boundaries <- unionSpatialPolygons(catchment_areas, catchment_areas$town)

# Prepare related data
catchment_areas_df <- data.frame(unique(catchment_area_set_final[, .(town, group, intervention_date, site_type)]))
row.names(catchment_areas_df) <- catchment_areas_df$town

# Promote back to SPDF
catchment_area_boundaries_data <- SpatialPolygonsDataFrame(catchment_area_boundaries, catchment_areas_df, match.ID = TRUE)

# save catchment areas
save(catchment_area_boundaries_data, file = "data/catchment area boundaries spatial.Rda")


ed_sites_spatial@data$intervention_site <- FALSE
ed_sites_spatial@data$intervention_site[ed_sites_spatial$site_name %in% c("bishop auckland general", "hemel hempstead", "newark", "rochdale infirmary", "university hospital of hartlepool")] <- TRUE

pal1 = colorFactor(c("#7fc97f","#beaed4","#fdc086","#ffff99","#386cb0"), domain = unique(catchment_area_boundaries_data$group))
pal2 = colorFactor(c("#e41a1c", "#111111"), domain = c(FALSE, TRUE))
leaflet() %>% addTiles() %>%
  addPolygons(data = catchment_area_boundaries_data[catchment_area_boundaries_data$site_type == "intervention", ], stroke = TRUE, color = "#666666", weight = 1, fillColor = ~pal1(group), opacity = 1, fillOpacity = 0.7, popup = ~paste0(town, " - ", group)) %>%
  addCircleMarkers(data = ed_sites_spatial[((ed_sites_spatial$type1_ae_2012 == FALSE & ed_sites_spatial$type1_ae_2012 == FALSE) | ed_sites_spatial$type1_ae_2012 == TRUE) & ed_sites_spatial$children_only == FALSE, ], stroke = TRUE, weight = 1, color = "#666666", radius = 4, fillColor = ~pal2(intervention_site), fillOpacity = 0.7, popup = ~site_name)
