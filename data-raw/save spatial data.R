library(data.table)
library(rgdal)
library(rgeos)
library(openxlsx)

# Projection strings
WGS84_projection_str <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
OSGB36_projection_str <- "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +datum=OSGB36 +units=m +no_defs +ellps=airy +towgs84=446.448,-125.157,542.060,0.1502,0.2470,0.8421,-20.4894"

# A&E Sites ---------------------------------------------------------------

## Read in A&E site locations
site_spatial_data <- fread("data-raw/geography data/Hospital sites/2012-2015 British A&E sites - 20151124.csv", header = TRUE, colClasses = "character")

# Select A&E sites to display
site_spatial_data <- site_spatial_data[(children.only.2012 == "FALSE" | is.na(children.only.2012)) & (children.only.2015 == "FALSE" | is.na(children.only.2015)), ]


## Convert to spatial data

# Convert to spatial data type
coordinates(site_spatial_data) <- site_spatial_data[, .(as.integer(easting), as.integer(northing))]

# Set projection
proj4string(site_spatial_data) <- CRS(OSGB36_projection_str)

# Transform to WGS84 coordinate system
ae_site_spatial_data <- spTransform(site_spatial_data, WGS84_projection_str)

# Save
save(ae_site_spatial_data, file = "data/ae site spatial data.Rda", compress = "xz")

# Remove from memory
rm(site_spatial_data, ae_site_spatial_data)
gc()


# ONS LSOA 2001 boundary data ---------------------------------------------

lsoa_boundary_data_raw <- readOGR("data-raw/geography data/Boundaries/Lower_layer_super_output_areas_(E+W)_2001_Boundaries_(Generalised_Clipped)_V2", "LSOA_2001_EW_BGC_V2")

# Keep only English LSOAs
lsoa_boundary_data_raw <- lsoa_boundary_data_raw[substr(lsoa_boundary_data_raw$LSOA01CD, 1, 1) == "E",]

# Transform to WGS84 coordinate system
lsoa_boundary_data <- spTransform(lsoa_boundary_data_raw, WGS84_projection_str)

# save
save(lsoa_boundary_data, file = "data/lsoa boundary data.Rda", compress = "xz")

# Remove from memory
rm(lsoa_boundary_data_raw, lsoa_boundary_data)
gc()



# ONS LSOA 2001 population weighted centroid data -------------------------

lsoa_centroid_data_raw <- readOGR("data-raw/geography data/Centroids", "LSOA_2001_EW_PWC")

# Keep only English LSOAs
lsoa_centroid_data_raw <- lsoa_centroid_data_raw[substr(lsoa_centroid_data_raw$LSOA01CD, 1, 1) == "E",]

# Transform to WGS84 coordinate system
lsoa_centroid_data <- spTransform(lsoa_centroid_data_raw, WGS84_projection_str)

# save
save(lsoa_centroid_data, file = "data/lsoa centroids data.Rda", compress = "xz")

# Remove from memory
rm(lsoa_centroid_data_raw, lsoa_centroid_data)
gc()

# tools::checkRdaFiles("data/lsoa centroids data.Rda")
# tools::resaveRdaFiles("data/lsoa centroids data.Rda")
# tools::checkRdaFiles("data/lsoa centroids data.Rda")


# NHS England 2015 CCG Ambulance Service boundaries -------------------------

ccg2015_boundary_data <- readOGR("data-raw/geography data/Boundaries/Clinical_commissioning_groups_(Eng)_Jul_2015_Boundaries_(Generalised_Clipped)_V2", "CCG_JUL_2015_EN_BGC_V2")

# Transform to WGS84 coordinate system
ccg2015_boundary_data <- spTransform(ccg2015_boundary_data, WGS84_projection_str)

# Read in NHS Engalnd Ambulance service to ccg 2015 lookup
ambulance_service_ccg2015_lookup <- data.table(read.xlsx("D:/Rpackages/rclosed/data-raw/geography data/Lookups/NHS England Ambulance Service boundaries/CCG to ambulance service lookup - Jan 2016.xlsx", startRow = 4L, colNames = FALSE))
ambulance_service_ccg2015_lookup[, X2 := NULL]
setnames(ambulance_service_ccg2015_lookup, c("CCG15CD", "ambulance_service"))

# Merge in NHS England Ambulance Service lookup
ccg2015_boundary_data_amb <- merge(ccg2015_boundary_data, ambulance_service_ccg2015_lookup, by = "CCG15CD", all.x = TRUE)
#ccg2015_boundary_data$ambulance_service <- as.character(ccg2015_boundary_data$ambulance_service)

# Generate ambulance service boundaries
ambulance_service_boundaries_2015 <- gUnaryUnion(ccg2015_boundary_data_amb, id = ccg2015_boundary_data_amb$ambulance_service)

# Promote to SPDF, fix labelling
ambulance_service_boundaries_2015 <- SpatialPolygonsDataFrame(ambulance_service_boundaries_2015, data.frame(ambulance_service = row.names(ambulance_service_boundaries_2015), row.names = row.names(ambulance_service_boundaries_2015), stringsAsFactors = FALSE))

# Save
save(ambulance_service_boundaries_2015, file = "data/ambulance service boundaries 2015.Rda", compress = "xz")

# Remove from memory
rm(ccg2015_boundary_data, ccg2015_boundary_data_amb, ambulance_service_ccg2015_lookup, ambulance_service_boundaries_2015)
gc()



# GB Country Boundaries ---------------------------------------------------

england_boundary_data <- readOGR("data-raw/geography data/Boundaries/Countries_December_2015_Ultra_Generalised_Clipped_Boundaries_in_Great_Britain", "Countries_December_2015_Ultra_Generalised_Clipped_Boundaries_in_Great_Britain")

# Transform to WGS84 coordinate system
england_boundary_data <- spTransform(england_boundary_data, WGS84_projection_str)

# Keep only England
england_boundary_data <- england_boundary_data[substr(england_boundary_data$ctry15cd, 1, 1) == "E", ]

# Save
save(england_boundary_data, file = "data/england boundary 2015.Rda", compress = "xz")

# tools::checkRdaFiles("data/england boundary 2015.Rda")
# tools::resaveRdaFiles("data/england boundary 2015.Rda")
# tools::checkRdaFiles("data/england boundary 2015.Rda")
