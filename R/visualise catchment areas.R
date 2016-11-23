library(leaflet)
library(rgdal)
library(maptools)

# load catchment areas
load("data/catchment area set final.Rda")

# load spatial data
## LSOA boundaries
load("data/lsoa boundary data.Rda")
## site locations
load("data/ae site spatial data.Rda")

# create catchment area boundaries
catchment_areas <- merge(lsoa_boundary_data, catchment_area_set_final, by.x = "LSOA01CD", by.y = "lsoa", all.x = FALSE, duplicateGeoms = TRUE)
ed_sites <- merge(ae_site_spatial_data, catchment_area_set_final, by.x = "LSOA01CD", by.y = "lsoa", all.x = FALSE)
rm(lsoa_boundary_data)

# Union of LSOAs into catchment areas, returns SP (not SPDF)
catchment_area_boundaries <- unionSpatialPolygons(catchment_areas, catchment_areas$town)

# Prepare related data
catchment_areas_df <- data.frame(unique(catchment_area_set_final[, .(town, group, intervention_date, site_type)]))
row.names(catchment_areas_df) <- catchment_areas_df$town

# Promote back to SPDF
catchment_area_boundaries_data <- SpatialPolygonsDataFrame(catchment_area_boundaries, catchment_areas_df, match.ID = TRUE)

pal1 = colorFactor(c("#1b9e77","#d95f02","#7570b3","#e7298a","#66a61e"), domain = unique(catchment_area_boundaries_data$group))
pal2 = colorFactor(c("#e41a1c", "#4daf4a", "#377eb8"), domain = c("intervention", "matched control", "pooled control"))
leaflet() %>% addTiles() %>% addPolygons(data = catchment_area_boundaries_data, color = ~pal2(site_type), weight = 2, fillColor = ~pal1(group), opacity = 1, fillOpacity = 0.7, popup = ~paste0(town, " - ", group))
