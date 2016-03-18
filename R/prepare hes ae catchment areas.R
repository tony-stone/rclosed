# Still need to clean this up into project style file

library(data.table)

generateCatchmentAreaSets <- function(end_date_exc, period_length_months = 12) {
  # Function to generate catchment areas,
  # volumes_by_month_destination_lsoa
  #                       - required global data.table with volumes by 'month', 'destination', 'lsoa' (field names, as character type, month as 'end_date_exc', below) [data.table type]
  # end_date_exc          - year and month of the first month (after start.dat.inc) that we wish to exclude [Date type]
  # period_length_months  - how long (months) before end_date_exc to look [integer; defaults to 12]

  # Calculate start date based on period
  start_date_inc <- end_date_exc
  lubridate::month(start_date_inc) <- lubridate::month(start_date_inc) - period_length_months

  # Keep only rows within the time period of interest and sum the volumes by distinct (destination, lsoa)-pairs over the time period of interest
  volume_data_summed_over_period <- volume_by_month_destination_lsoa[yearmonth >= start_date_inc & yearmonth < end_date_exc, .(N = sum(volume)), by=c("destination", "lsoa")]

  # For each lsoa, rank volume (number of attendances/admissions/calls) - if tied, ranks are allocated randomly
  volume_data_summed_over_period[, destination_rank := rank(N, ties.method = "random"), by = lsoa]

  # Create inverted rank/rownum and total volume fields
  volume_data_summed_over_period[, ':=' ( destination_rank = max(destination_rank) - destination_rank + 1L,
    total_volume = as.integer(sum(N))),
    by = lsoa]
  ## Keep only first and second ranked destinations for each LSOA, subject to following rules applied in the specified order:
  top2_destinations_by_lsoa <- volume_data_summed_over_period[destination_rank == 1 | destination_rank == 2, ]

  # Attach intervention_date and period_length
  top2_destinations_by_lsoa[, ':=' (intervention_date = end_date_exc,
    period_lenth = period_length_months)]

  return(top2_destinations_by_lsoa)
}






  # Convert to wide form (this handles LSOAs with no second destination) and identify ties
  top2_destinations_by_lsoa <- dcast(top2_destinations_by_lsoa, lsoa + total_volume ~ destination_rank, value.var = c("N", "destination"), fill = 0L)
  top2_destinations_by_lsoa[, tied := FALSE]
  top2_destinations_by_lsoa[N_1 == N_2, tied := TRUE]
  top2_destinations_by_lsoa[N_2 == 0, destination_2 := NA]


  # Calc fraction of attendances at any destination from this LSOA, to the specific destination; diff between volume between first and second destination (as fraction); and remove unnecessary fields
  top2_destinations_by_lsoa[, ':=' (frac_to_destination = N_1 / total_volume,
    diff_first_second = N_1 - N_2,
    N_2 = NULL)][, ':=' (diff_first_second_frac = diff_first_second / total_volume,
      frac_to_destination_grp = cut(frac_to_destination, breaks = 0:10/10, include.lowest = TRUE, right = FALSE))]

  # Sort names
  setnames(top2_destinations_by_lsoa, c("N_1", "destination_1"), c("N", "destination"))


attachTrustNames <- function(data, field_name) {
  load("data/trust names data.Rda")
  return(merge(data, trust_names_data[, c("open_date", "close_date") := NULL], by.x = field_name, by.y = "trust_code", all.x = TRUE))






# Control data ------------------------------------------------------------

# read
load("data/site data.Rda")

# get periods for which to calc catchment areas
intervention_dates <- unique(site_data[ambulance_service == "EMAS", intervention_date])

# Create our catchment areas for different durations
catchment_area_sets_list <- list()
for(period_length in seq(6, 24, 6)) {
  catchment_area_sets_list <- c(catchment_area_sets_list, lapply(intervention_dates, generateCatchmentAreaSets, period_length_months = period_length))
}

# Bind data together
catchment_area_sets <- rbindlist(catchment_area_sets_list)

# Remove unnecessary data vars
rm(catchment_area_sets_list)
gc()

# Attach trust names
catchment_area_sets <- attachTrustNames(catchment_area_sets, "destination")
catchment_area_sets <- attachTrustNames(catchment_area_sets, "destination_2")
setnames(catchment_area_sets, c("trust_name.x", "trust_name.y"),  c("destination_name", "destination_second_name"))

## Attach intervention/control details
# Attach for which closure the catchment is relevant
catchment_area_sets <- merge(catchment_area_sets, unique(site_data[ambulance_service == "EMAS", .(group, intervention_date)]), by = "intervention_date", all.x = TRUE)

# Attach site names (where relevant)
catchment_area_sets <- merge(catchment_area_sets, unique(site_data[ambulance_service == "EMAS", .(intervention_date, trust_code, town, is_single_ED_trust)]), by.x = c("intervention_date", "destination"), by.y = c("intervention_date", "trust_code"), all.x = TRUE)


library(ggplot2)
library(scales)
library(RColorBrewer)


# Check data quality ------------------------------------------------------

data_quality_check <- copy(catchment_area_sets[1, catchment_areas][[1]])

## Fraction to destination
# Add tick marks at specific points in the distribution
brks <- c( min(data_quality_check$frac_to_destination),
  quantile(data_quality_check$frac_to_destination, 0.05)[[1]], # 5th percentile
  quantile(data_quality_check$frac_to_destination, 0.5)[[1]], # median
  mean(data_quality_check$frac_to_destination),
  quantile(data_quality_check$frac_to_destination, 0.95)[[1]], # 95th percentile
  max(data_quality_check$frac_to_destination) )

# Plot all
ggplot(data_quality_check, aes(frac_to_destination)) +
  geom_histogram(aes(y = ..count.. / sum(..count..)), binwidth = 0.01) +
  coord_cartesian(xlim = c(0, 1)) +
  scale_x_continuous(breaks = brks, labels = paste0(round(brks, 2), c(" (min)", " (5th percentile)", " (median)", " (mean)", " (95th percentile)", " (max)") )  ) +
  scale_y_continuous(name = "Normalised distribution", labels = percent) +
  theme(axis.text.x = element_text(face = "bold", angle = 90, hjust = 0.0, vjust = 0.3))


# Plot each destination
ggplot(data_quality_check, aes(frac_to_destination)) +
  geom_histogram(aes(y = ..count.. / tapply(..count.., ..PANEL.., sum)[..PANEL..]), binwidth=0.01) +
  facet_wrap(~ destination_name) +
  scale_x_continuous(expand = c(0, 0.005)) +
  scale_y_continuous(name = "Normalised distribution", labels = percent) +
  coord_cartesian(xlim = c(0, 1))

## Scatter difference between first and second as fraction AGAINST Total volume to any destination

# Plot
ggplot(data_quality_check, aes(total_volume, diff_first_second_destination_frac)) +
  geom_point() +
  facet_wrap(~ destination_name)
# RW6 [Rochdale Infirmary] looks sub-optimal


## LSOA's per site by fraction
ggplot(data_quality_check, aes(destination_name, fill = frac_to_destination_grp, order = -as.numeric(frac_to_destination_grp))) +
  geom_bar() +
  scale_fill_brewer(palette="PRGn", guide = guide_legend(reverse = TRUE)) +
  theme(axis.text.x = element_text(face = "bold", angle = 90, hjust = 0.0, vjust = 0.3))

## fraction to destination as a fraction of all LSOAs
ggplot(data_quality_check, aes(destination_name, fill = frac_to_destination_grp, order = -as.numeric(frac_to_destination_grp))) +
  geom_bar(position = "fill") +
  scale_fill_brewer(palette="PRGn", guide = guide_legend(reverse = TRUE)) +
  scale_y_continuous(labels = percent, expand = c(0, 0.005)) +
  theme(axis.text.x = element_text(face = "bold", angle = 90, hjust = 0.0, vjust = 0.3))







# Process spatial data ----------------------------------------------------

library(rgeos)
library(maptools)
library(rgdal)
library(leaflet)
library(htmltools)

# Projection strings
OSGB36_projection_str <- "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +datum=OSGB36 +units=m +no_defs"
WGS84_projection_str <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"

## Read in A&E site locations
site_spatial_data <- fread("data-raw/geography data/Hospital sites/2012-2015 British A&E sites - 20151124.csv", header = TRUE, colClasses = "character")

# Select A&E sites to display
site_spatial_data <- site_spatial_data[(children.only.2012 == "FALSE" | is.na(children.only.2012)) & (children.only.2015 == "FALSE" | is.na(children.only.2015)), ]

# Convert to spatial data type
coordinates(site_spatial_data) <- site_spatial_data[, .(as.integer(easting), as.integer(northing))]

# Set projection
proj4string(site_spatial_data) <- CRS(OSGB36_projection_str)

# Transform to WGS84 coordinate system
site_spatial_data <- spTransform(site_spatial_data, WGS84_projection_str)

## Read LSOA01 shape file for England+Wales
LSOA_spatial_data <- readShapePoly("data-raw/geography data/Boundaries/Lower_layer_super_output_areas_(E+W)_2001_Boundaries_(Generalised_Clipped)_V2/LSOA_2001_EW_BGC_V2.shp",
  proj4string=CRS(OSGB36_projection_str))

# Keep only English LSOAs
LSOA_spatial_data <- LSOA_spatial_data[substr(LSOA_spatial_data$LSOA01CD, 1, 1) == "E",]

# Transform to WGS84 coordinate system
LSOA_spatial_data <- spTransform(LSOA_spatial_data, WGS84_projection_str)

# Create set of polygons for which we have no HES data
load("data/lsoa 2001s in hes data.rda")
lsoa01s_in_HES <- data.table(lsoa = lsoa01s_in_HES, in_hes_data = TRUE)
LSOA_spatial_data <- merge(LSOA_spatial_data, lsoa01s_in_HES, by.x = "LSOA01CD", by.y = "lsoa")
LSOA_spatial_data_nodata <- LSOA_spatial_data[is.na(LSOA_spatial_data$in_hes_data), ]
LSOA_spatial_nodata_mask <- unionSpatialPolygons(LSOA_spatial_data_nodata, rep(1, nrow(LSOA_spatial_data_nodata)))

# Remove polygons for which we have no data
LSOA_spatial_data_relevant <- LSOA_spatial_data[!is.na(LSOA_spatial_data$in_hes_data), ]


# Look at our catchment areas ---------------------------------------------


areas_to_map <- catchment_area_sets[1, catchment_areas][[1]]

# Merge data into map
LSOA_spatial_hes_data <- merge(LSOA_spatial_data_relevant, areas_to_map, by.x = "LSOA01CD", by.y = "lsoa")

# Create set of polygons which are not in the catchment areas for the sites of interest
LSOA_spatial_data_blanks <- LSOA_spatial_hes_data[is.na(LSOA_spatial_hes_data$destination), ]

# dissolve borders
#LSOA_spatial_data_mask <- unionSpatialPolygons(LSOA_spatial_data_blanks, rep(1, nrow(LSOA_spatial_data_blanks)))

LSOA_spatial_hes_data_areas <- LSOA_spatial_hes_data[!is.na(LSOA_spatial_hes_data$destination), ]

# Get colours
palette <- brewer.pal(10, "PRGn")

# Add additional fields for presenting as map
LSOA_spatial_hes_data_areas$fill_colour <- palette[11 - as.integer(LSOA_spatial_hes_data_areas$frac_to_destination_grp)]
LSOA_spatial_hes_data_areas$caption <- paste0("<h1>Data</h1><ul><li>", htmlEscape(paste0("Destination: ", LSOA_spatial_hes_data_areas$destination_name, " (", LSOA_spatial_hes_data_areas$destination, ")")), "</li>",
  "<li>", htmlEscape(paste0("Second destination: ", LSOA_spatial_hes_data_areas$destination_second_name, " (", LSOA_spatial_hes_data_areas$destination_second, ")")), "</li>",
  "<li>", htmlEscape(paste0("Total attendances: ", LSOA_spatial_hes_data_areas$total_volume)), "</li>",
  "<li>", htmlEscape(paste0("Fraction: ", round(LSOA_spatial_hes_data_areas$frac_to_destination * 100), "%")), "</li>",
  "<li>", htmlEscape(paste0("Fraction between first and second: ", round(LSOA_spatial_hes_data_areas$diff_first_second_destination_frac * 100), "%")), "</li>",
  "</ul>")

# Display map
leaflet() %>% addTiles() %>% addMarkers(data = site_spatial_data, popup = ~site.name) %>%
  # addPolygons(data = LSOA_spatial_data_mask, stroke = FALSE, color = "#fff", weight = 1, opacity = 1, fillColor = "#fff", fillOpacity = .8) %>%
  # addPolygons(data = LSOA_spatial_nodata_mask, stroke = FALSE, color = "#000", weight = 1, opacity = 1, fillColor = "#000", fillOpacity = .9) %>%
  addPolygons(data = LSOA_spatial_hes_data_areas, stroke = TRUE, color = "#000", weight = 1, popup = ~caption, opacity = 1, fillColor = ~fill_colour, fillOpacity = .8)
