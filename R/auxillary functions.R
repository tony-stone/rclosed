createMeasureFilename <- function(m_name, geo_level = "lsoa") {
  g_lev <- ifelse(geo_level == "lsoa", "lsoa", "site")
  return(paste0("data/", m_name, " measure - ", g_lev, " - ", format(Sys.time(), "%Y-%m-%d %H.%M"), ".Rda"))
}





fillDataPoints <- function(data) {
  # Get catchment areas data
  load("data/catchment area set final.Rda")

  # Remove invalid time points from the data
  data <- data[yearmonth >= as.Date("2007-04-01") & yearmonth <= as.Date("2014-03-01")]

  # Create datapoints for each lsoa/month/measure/sub_measure covering the measurement space (as this is not guaranteed from the data)
  data_points <- data.table::data.table(expand.grid(lsoa = unique(catchment_area_set_final$lsoa), yearmonth = seq(as.Date("2007-04-01"), as.Date("2014-03-01"), by = "month"), measure = unique(data$measure), sub_measure = unique(data$sub_measure), stringsAsFactors = FALSE))

  # Merge data into data points so we have a record/row for each and every point
  data_all_points <- merge(data_points, data, by = c("lsoa", "yearmonth", "measure", "sub_measure"), all = TRUE)
  # if we do not have a value for a data point within the period for which we have data, set this to 0
  # (else, if outside period for which we have data, it will remain as NA)
  data_all_points[is.na(value) & yearmonth >= min(data$yearmonth) & yearmonth <= max(data$yearmonth), value := 0]

  # Merge in catchment area data
  data_measure_all <- merge(data_all_points, catchment_area_set_final, by = "lsoa", all = TRUE)

  # Label intervention month "24" and each other month relative to that
  data_measure_all[, relative_month := as.integer(lubridate::interval(intervention_date, yearmonth) %/% months(1)) + 25L]

  # Remove data outwith 2 years of intervention
  data_measure <- data_measure_all[relative_month >= 1L & relative_month <= 48L]

  # Prepare time_to_ed field/variable and finalise dataset
  data_measure[, diff_time_to_ed := 0L]
  data_measure[site_type == "intervention" & yearmonth >= intervention_date, diff_time_to_ed := diff_first_second]
  data_measure[, c("diff_first_second", "intervention_date", "ae_post_intv") := NULL]

  return(data_measure)
}



addFractionSubmeasure <- function(data, subm_denom, subm_num, subm_new) {

  if("lsoa" %in% colnames(data)) {
    wide_data <- merge(data[sub_measure == subm_denom, .(lsoa, yearmonth, relative_month, measure, town, group, site_type, diff_time_to_ed, denominator = value)],
      data[sub_measure == subm_num, .(lsoa, yearmonth, relative_month, measure, town, group, site_type, diff_time_to_ed, numerator = value)],
      by = c("lsoa", "yearmonth", "measure", "town", "group", "site_type", "relative_month", "diff_time_to_ed"))
  } else {
    wide_data <- merge(data[sub_measure == subm_denom, .(yearmonth, relative_month, measure, town, group, site_type, denominator = value)],
      data[sub_measure == subm_num, .(yearmonth, relative_month, measure, town, group, site_type, numerator = value)],
      by = c("yearmonth", "measure", "town", "group", "site_type", "relative_month"))
  }

  wide_data[, ':=' (value = numerator / denominator,
    sub_measure = subm_new,
    numerator = NULL,
    denominator = NULL)]

  # When the denominator is 0, we get NA. Set to 0 instead.
  wide_data[is.na(value), value := 0]

  return(rbind(data, wide_data))
}


collapseLsoas2Sites <- function(data, remove_nas = FALSE) {
  return(data[, .(value = sum(value, na.rm = remove_nas)), by = .(yearmonth, measure, sub_measure, town, group, site_type, relative_month)])
}


getOptimalCompress <- function(fname) {
  tools::resaveRdaFiles(fname)
  return(tools::checkRdaFiles(fname))
}
