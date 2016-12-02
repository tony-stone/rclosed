library(data.table)
library(rgdal)
library(maptools)
library(ggplot2)
library(ggmap)
library(ggrepel)



saveCatchmentAreaBoundaries <- function() {
  # load catchment areas
  load("data/catchment area set final.Rda")

  # load spatial data
  ## LSOA boundaries
  load("data/lsoa boundary data.Rda")

  catchment_area_set <- catchment_area_set_final[site_type == "intervention"]
  catchment_area_set[, time_to_next_quintile := cut(diff_first_second, stats::quantile(catchment_area_set$diff_first_second, probs = 0:5 / 5, na.rm = TRUE, names = FALSE, type = 8), labels = FALSE, include.lowest = TRUE)]

  # create catchment areas
  lsoa_boundary_data@data["LSOA01CD"] <- as.character(lsoa_boundary_data$LSOA01CD)
  lsoa_boundary_data@data["LSOA01NM"] <- NULL
  lsoa_boundary_data@data["LSOA01NMW"] <- NULL
  row.names(lsoa_boundary_data) <- lsoa_boundary_data$LSOA01CD
  intervention_catchment_areas <- merge(lsoa_boundary_data, catchment_area_set, by.x = "LSOA01CD", by.y = "lsoa", all.x = FALSE, duplicateGeoms = TRUE)

  # save catchment areas
  save(intervention_catchment_areas, file = "data/intervention catchment areas spatial.Rda")

  ## create catchment area boundaries
  # Union of LSOAs into catchment areas, returns SP (not SPDF)
  catchment_area_boundaries <- unionSpatialPolygons(intervention_catchment_areas, intervention_catchment_areas$town)

  # Prepare related data
  catchment_areas_df <- data.frame(unique(catchment_area_set_final[site_type == "intervention", .(town, group, intervention_date, site_type)]))
  row.names(catchment_areas_df) <- catchment_areas_df$town

  # Promote back to SPDF
  intervention_catchment_boundaries <- SpatialPolygonsDataFrame(catchment_area_boundaries, catchment_areas_df, match.ID = TRUE)

  # save catchment areas
  save(intervention_catchment_boundaries, file = "data/intervention catchment boundaries spatial.Rda")
}


# saveCatchmentAreaBoundaries()

load("data/intervention catchment areas spatial.Rda")
load("data/intervention catchment boundaries spatial.Rda")
load("data/british ed sites spatial.Rda")



# Process ED sites --------------------------------------------------------

# Only keep relevant EDs
ed_sites_spatial <- ed_sites_spatial[((ed_sites_spatial$type1_ae_2012 == FALSE & ed_sites_spatial$type1_ae_2015 == FALSE) | ed_sites_spatial$type1_ae_2012 == TRUE) &
    ed_sites_spatial$english == TRUE &
    ed_sites_spatial$children_only == FALSE &
    ed_sites_spatial$site_name != "newcastle general" &
    ed_sites_spatial$site_name != "st marys (cornwall partnership nhs foundation trust)" &
    ed_sites_spatial$site_name != "derbyshire royal infirmary", ]

# Classify EDs
ed_sites_spatial@data$site_type <- "other ED"
ed_sites_spatial@data$site_type[ed_sites_spatial$site_name %in% c("bishop auckland general", "hemel hempstead", "newark", "rochdale infirmary", "university hospital of hartlepool")] <- "intervention ED"


ed_sites <- data.frame(ed_sites_spatial)
names(ed_sites)[names(ed_sites) == "V1"] <- "long"
names(ed_sites)[names(ed_sites) == "V2"] <- "lat"



# Process catchment areas -------------------------------------------------

catchment_areas <- fortify(intervention_catchment_areas)
catchment_areas <- merge(intervention_catchment_areas@data, catchment_areas, by.x = "LSOA01CD", by.y = "id")
catchment_boundaries <- fortify(intervention_catchment_boundaries)
catchment_boundaries <- merge(intervention_catchment_boundaries@data, catchment_boundaries, by.x = "town", by.y = "id")


# # Calculate minimum bounds in which all boundary vertices are contained
# location_bounds_auto <- do.call(rbind, lapply(unique(catchment_areas$id), function(x, data) {
#   return(data.frame(town = x, left = min(data$long[data$id == x]), bottom = min(data$lat[data$id == x]), right =  max(data$long[data$id == x]), top = max(data$lat[data$id == x]), stringsAsFactors = FALSE))
# }, data = catchment_areas))


print(getSiteMap("Bishop Auckland", catchment_areas, catchment_boundaries, ed_sites))
print(getEnglandMap(catchment_areas, ed_sites))


getSiteMap <- function(town, catchment_areas, catchment_boundaries, ed_sites) {

  # Nice bounds (by eye)
  location_bounds <- data.frame(
    town = c("Bishop Auckland", "Hartlepool", "Hemel Hempstead", "Newark", "Rochdale"),
    left = c(-3.00, -1.68, -0.85, -1.55, -2.60),
    bottom = c(54.00, 54.53, 51.57, 52.60, 53.50),
    right = c(-1.27, -1.00, -0.15, -0.15, -1.70),
    top = c(54.99, 54.85, 51.90, 53.35, 53.95))

mymap <- get_map(location = unlist(location_bounds[location_bounds$town == town, 2:5]), source = "stamen", maptype = "toner-background")

return(ggmap(mymap, base_layer=ggplot(aes(x=lon,y=lat), data=zips), extent = "normal", maprange = FALSE) +
  coord_map(projection = "mercator",
    xlim = c(attr(mymap, "bb")$ll.lon, attr(mymap, "bb")$ur.lon),
    ylim = c(attr(mymap, "bb")$ll.lat, attr(mymap, "bb")$ur.lat)) +
#  geom_polygon(data = catchment_areas[catchment_areas$town == town,], aes(x = long, y = lat, group = group.y, fill = diff_first_second), size = 0, alpha = .75) +
#   scale_fill_gradient(low = "#f7fcf0", high = "#084081", limits = c(0, 25),
#     guide = guide_legend(title = "Increased time to next\nnearest ED (minutes)")) +
  geom_polygon(data = catchment_areas[catchment_areas$town == town,], aes(x = long, y = lat, group = group.y, fill = as.character(time_to_next_quintile)), size = 0, alpha = .75) +
  scale_fill_manual(values = c("1" = '#f0f9e8',
    "2" = "#bae4bc",
    "3" = "#7bccc4",
    "4" = "#43a2ca",
    "5" = "#0868ac"),
    labels = c("1 (least increase)", 2:4, "5 (greatest increase)"),
    limits = as.character(1:5),
    guide = guide_legend(title = "Increased time to next\nnearest ED (quintiles)")) +
  geom_point(data = ed_sites, aes(x = long, y = lat, shape = site_type), colour = "#000000", size = 3) +
  scale_shape_manual(values = c("intervention ED" = 1, "other ED" = 15), guide = guide_legend(title = "ED site type")) +
  geom_path(data = catchment_boundaries[catchment_boundaries$town == town,], aes(x = long, y = lat, group = group.y), show.legend = FALSE, colour = "#666666", size = 1) +
  geom_label_repel(data = ed_sites[attr(mymap, "bb")$ll.lon <= ed_sites$long & ed_sites$long <= attr(mymap, "bb")$ur.lon & attr(mymap, "bb")$ll.lat <= ed_sites$lat & ed_sites$lat <= attr(mymap, "bb")$ur.lat, ], aes(x = long, y = lat, label = hospital_name),
    fontface = 'bold',
    box.padding = unit(0.35, "lines"),
    point.padding = unit(0.5, "lines"),
    segment.color = "#000000",
    segment.size = 1) +
  theme_void() +
  theme(legend.justification = c(1, 0), legend.position = c(1, 0), legend.background = element_rect(colour = '#999999', fill = 'white', size = 0.5)))
}


# National plot
getEnglandMap <- function(catchment_areas, ed_sites) {
  load("data/england boundary 2015.Rda")
  england_boundary <- fortify(england_boundary_data)

  return(ggplot() +
  geom_polygon(data = england_boundary, aes(x = long, y = lat, group = group), colour = "#666666", fill = "#f2fff3") +
  geom_polygon(data = catchment_areas, aes(x = long, y = lat, group = group.y, fill = town)) +
  scale_fill_manual(values = c("Bishop Auckland" = "#66c2a5",
    "Hartlepool" = "#fc8d62",
    "Hemel Hempstead" = "#8da0cb",
    "Newark" = "#e78ac3",
    "Rochdale" = "#a6d854")) +
  geom_point(data = ed_sites, aes(x = long, y = lat, shape = site_type), colour = "#000000", size = 1.5) +
  scale_shape_manual(values = c("intervention ED" = 1, "other ED" = 15)) +
  theme_void() +
  coord_map() +
  theme(legend.justification = c(0, 1), legend.position = c(-0.1, 0.95), legend.background = element_rect(colour = '#999999', fill = 'white', size = 0.5)))
}
