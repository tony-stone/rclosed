prepareEdSites <- function() {
  # Projection strings
  WGS84_projection_str <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
  OSGB36_projection_str <- "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +datum=OSGB36 +units=m +no_defs +ellps=airy +towgs84=446.448,-125.157,542.060,0.1502,0.2470,0.8421,-20.4894"

  # load data
  load("data/nhs postcode directory.Rda")
  load("data/british ed sites.Rda")

  ed_sites_spatial <- merge(ed_sites, nhs_postcode_directory[, .(postcode, easting, northing)], by = "postcode", all.x = TRUE)
  stopifnot(!any(c(is.na(ed_sites_spatial$easting), is.na(ed_sites_spatial$northing))))

  ## Convert to spatial data
  sp::coordinates(ed_sites_spatial) <- ed_sites_spatial[, .(as.integer(easting), as.integer(northing))]

  # Set projection
  sp::proj4string(ed_sites_spatial) <- sp::CRS(OSGB36_projection_str)

  # Transform to WGS84 coordinate system
  ed_sites_spatial <- sp::spTransform(ed_sites_spatial, WGS84_projection_str)

  # Save
  save(ed_sites_spatial, file = "data/british ed sites spatial.Rda", compress = "xz")
}
