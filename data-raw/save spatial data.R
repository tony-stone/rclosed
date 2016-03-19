library(data.table)
library(maptools)

# Projection strings
OSGB36_projection_str <- "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +towgs84=446.448,-125.157,542.06,0.15,0.247,0.842,-20.489 +units=m +vunits=m +no_defs"
WGS84_projection_str <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"


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



# ONS LSOA 2001 data ------------------------------------------------------

lsoa_spatial_data <- readShapePoly("data-raw/geography data/Boundaries/Lower_layer_super_output_areas_(E+W)_2001_Boundaries_(Generalised_Clipped)_V2/LSOA_2001_EW_BGC_V2.shp",
  proj4string=CRS(OSGB36_projection_str))

# Keep only English LSOAs
lsoa_spatial_data <- lsoa_spatial_data[substr(lsoa_spatial_data$LSOA01CD, 1, 1) == "E",]

# Transform to WGS84 coordinate system
lsoa_spatial_data <- spTransform(lsoa_spatial_data, WGS84_projection_str)

# save
save(lsoa_spatial_data, file = "data/lsoa spatial data.Rda", compress = "xz")

# tools::resaveRdaFiles("data/lsoa spatial data.Rda")
# tools::checkRdaFiles("data/lsoa spatial data.Rda")
