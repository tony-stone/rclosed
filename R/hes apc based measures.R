save_emergency_admissions_measure <- function() {

  db_conn <- connect2DB()

  tbl_name <- "emergency_admissions_by_diagnosis_site_lsoa_month"
  add_logic <- ""
  add_fields <- "startage, diag_01, diag_02, cause"

  # Prepare query string to create temp table
  sql_create_tbl <- getSqlUpdateQuery("apc", tbl_name, add_logic, add_fields)

  # Takes ~30s
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_tbl)

  # retrieve data
  emergency_admissions_by_diagnosis_site_lsoa_month <- getDataFromTempTable(db_conn, tbl_name, "apc", add_fields)

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  # Classify
  emergency_admissions_by_diagnosis_site_lsoa_month <- classifyAvoidableAdmissions(emergency_admissions_by_diagnosis_site_lsoa_month)

  # collapse to attendances by (lsoa, month, avoidable condition)
  emergency_admissions_by_condition_lsoa_month <- emergency_admissions_by_diagnosis_site_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth, condition)]

  # format
  setnames(emergency_admissions_by_condition_lsoa_month, "condition", "sub_measure")

  # Free up memory
  rm(emergency_admissions_by_diagnosis_site_lsoa_month)
  gc()

  # all emergency admissions
  emergency_admissions_by_lsoa_month_all <- emergency_admissions_by_condition_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth)]
  emergency_admissions_by_lsoa_month_all[, ':=' (measure = "avoidable emergency admissions",
    sub_measure = "all admissions")]

  # avoidable emergency admissions
  emergency_admissions_by_condition_lsoa_month_avoidable <- emergency_admissions_by_condition_lsoa_month[sub_measure != "other"]
  emergency_admissions_by_condition_lsoa_month_avoidable[, measure := "avoidable emergency admissions"]
  emergency_admissions_by_lsoa_month_avoidable <- emergency_admissions_by_condition_lsoa_month_avoidable[, .(value = sum(value)), by = .(lsoa, yearmonth, measure)]
  emergency_admissions_by_lsoa_month_avoidable[, sub_measure := "any"]
  emergency_admissions_by_lsoa_month <- rbind(emergency_admissions_by_lsoa_month_all, emergency_admissions_by_condition_lsoa_month_avoidable, emergency_admissions_by_lsoa_month_avoidable)

  # format
  emergency_admissions_measure <- fillDataPoints(emergency_admissions_by_lsoa_month)

  # Treat "all emergency admissions" as separate measure in same file
  emergency_admissions_measure[sub_measure == "all admissions", ':=' (measure = "all emergency admissions",
    sub_measure = as.character(NA))]

  # Collapse to site level
  emergency_admissions_site_measure <- collapseLsoas2Sites(emergency_admissions_measure)

  # save
  save(emergency_admissions_measure, file = createMeasureFilename("emergency admissions"), compress = "xz")
  save(emergency_admissions_site_measure, file = createMeasureFilename("emergency admissions", "site"), compress = "bzip2")
}



save_critical_care_stays_measure <- function() {

  db_conn <- connect2DB()

  tbl_name <- "cips_by_cc_site_lsoa_month"
  add_logic <- "cips_finished = TRUE AND nights_admitted < 184"
  add_fields <- "any_critical_care"

  # Prepare query string to create temp table
  sql_create_tbl <- getSqlUpdateQuery("apc", tbl_name, add_logic, add_fields)

  # Takes ~30s
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_tbl)

  # retrieve data
  cips_by_critical_care_site_lsoa_month <- getDataFromTempTable(db_conn, tbl_name, "apc", add_fields)

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  # collapse to patients by (lsoa, month, any_critical_care)
  cips_by_critical_care_lsoa_month <- cips_by_critical_care_site_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth, any_critical_care)]

  # collapse to patients by (lsoa, month)
  cips_by_lsoa_month <- cips_by_critical_care_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth)]
  cips_by_lsoa_month[, sub_measure := "all"]

  # collapse to patients by (lsoa, month, any_critical_care)
  cc_cips_by_lsoa_month <- cips_by_critical_care_lsoa_month[any_critical_care == 1, .(value = sum(value)), by = .(lsoa, yearmonth)]
  cc_cips_by_lsoa_month[, sub_measure := "critical care"]

  # combine "any critical care"- and all-  admissions by (lsoa, yearmonth)
  critical_care_cips_measure <- rbind(cips_by_lsoa_month, cc_cips_by_lsoa_month)
  critical_care_cips_measure[, measure := "critical care stays"]

  # format
  critical_care_cips_measure <- fillDataPoints(critical_care_cips_measure)
  critical_care_cips_site_measure <- collapseLsoas2Sites(critical_care_cips_measure)

  # Create proportion measure
  critical_care_cips_measure <- addFractionSubmeasure(critical_care_cips_measure, "all", "critical care", "fraction critical care")
  critical_care_cips_site_measure <- addFractionSubmeasure(critical_care_cips_site_measure, "all", "critical care", "fraction critical care")

  # save
  save(critical_care_cips_measure, file = createMeasureFilename("critical care stays"), compress = "bzip2")
  save(critical_care_cips_site_measure, file = createMeasureFilename("critical care stays", "site"), compress = "bzip2")
}



save_length_of_stay_measure <- function() {

  db_conn <- connect2DB()

  tbl_name <- "cips_by_nights_site_lsoa_month"
  add_logic <- "cips_finished = TRUE AND nights_admitted < 184"
  add_fields <- "nights_admitted"

  # Prepare query string to create temp table
  sql_create_tbl <- getSqlUpdateQuery("apc", tbl_name, add_logic, add_fields)

  # Takes ~30s
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_tbl)

  # retrieve data
  cips_by_nights_site_lsoa_month <- getDataFromTempTable(db_conn, tbl_name, "apc", add_fields)

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  # collapse to patients by (lsoa, month, nights_admitted)
  cips_by_nights_lsoa_month <- cips_by_nights_site_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth, nights_admitted)]

  # collapse to patients by (town, month, nights_admitted)
  cips_by_nights_lsoa_town_month <- attachTownForSiteLevelData(cips_by_nights_lsoa_month)
  cips_by_nights_town_month <- cips_by_nights_lsoa_town_month[, .(value = sum(value)), by = .(town, yearmonth, nights_admitted)]

  # get mean length of stay
  length_of_stay_mean <- cips_by_nights_lsoa_month[, .(value = getMeanFromBins(nights_admitted, value)), by = .(lsoa, yearmonth)]
  length_of_stay_mean_site <- cips_by_nights_town_month[, .(value = getMeanFromBins(nights_admitted, value)), by = .(town, yearmonth)]
  length_of_stay_mean[, sub_measure := "mean"]
  length_of_stay_mean_site[, sub_measure := "mean"]

  # get median length of stay
  length_of_stay_median <- cips_by_nights_lsoa_month[, .(value = getQuantileFromBins(nights_admitted, value, 0.5)), by = .(lsoa, yearmonth)]
  length_of_stay_median_site <- cips_by_nights_town_month[, .(value = getQuantileFromBins(nights_admitted, value, 0.5)), by = .(town, yearmonth)]
  length_of_stay_median[, sub_measure := "median"]
  length_of_stay_median_site[, sub_measure := "median"]

  # group
  length_of_stay_measure <- rbind(length_of_stay_mean, length_of_stay_median)
  length_of_stay_site_measure <- rbind(length_of_stay_mean_site, length_of_stay_median_site)
  length_of_stay_measure[, measure := "length of stay"]
  length_of_stay_site_measure[, measure := "length of stay"]

  # format
  length_of_stay_measure <- fillDataPoints(length_of_stay_measure, FALSE, TRUE)
  length_of_stay_site_measure <- fillDataPoints(length_of_stay_site_measure, FALSE, FALSE)

  # save
  save(length_of_stay_measure, file = createMeasureFilename("length of stay"), compress = "bzip2")
  save(length_of_stay_site_measure, file = createMeasureFilename("length of stay", "site"), compress = "bzip2")
}


save_hospital_transfers_measure <- function() {

  db_conn <- connect2DB()

  tbl_name <- "cips_by_ht_site_lsoa_month"
  add_logic <- "cips_finished = TRUE AND nights_admitted < 184"
  add_fields <- "any_transfer"

  # Prepare query string to create temp table
  sql_create_tbl <- getSqlUpdateQuery("apc", tbl_name, add_logic, add_fields)

  # Takes ~30s
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_tbl)

  # retrieve data
  cips_by_hospital_transfer_site_lsoa_month <- getDataFromTempTable(db_conn, tbl_name, "apc", add_fields)

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  # collapse to patients by (lsoa, month, any_hospital_transfer)
  cips_by_hospital_transfer_lsoa_month <- cips_by_hospital_transfer_site_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth, any_transfer)]

  # collapse to patients by (lsoa, month)
  cips_by_lsoa_month <- cips_by_hospital_transfer_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth)]
  cips_by_lsoa_month[, sub_measure := "all stays"]

  # collapse to patients by (lsoa, month, any_hospital_transfer)
  ht_cips_by_lsoa_month <- cips_by_hospital_transfer_lsoa_month[any_transfer == "t", .(value = sum(value)), by = .(lsoa, yearmonth)]
  ht_cips_by_lsoa_month[, sub_measure := "stays with transfer"]

  # combine "any hospital transfer"- and all-  admissions by (lsoa, yearmonth)
  hospital_transfer_cips_measure <- rbind(cips_by_lsoa_month, ht_cips_by_lsoa_month)
  hospital_transfer_cips_measure[, measure := "hospital transfers"]

  # format
  hospital_transfer_cips_measure <- fillDataPoints(hospital_transfer_cips_measure)
  hospital_transfer_cips_site_measure <- collapseLsoas2Sites(hospital_transfer_cips_measure)

  # Create proportion measure
  hospital_transfers_measure <- addFractionSubmeasure(hospital_transfer_cips_measure, "all stays", "stays with transfer", "fraction with transfer")
  hospital_transfers_site_measure <- addFractionSubmeasure(hospital_transfer_cips_site_measure, "all stays", "stays with transfer", "fraction with transfer")

  # save
  save(hospital_transfers_measure, file = createMeasureFilename("hospital transfers"), compress = "bzip2")
  save(hospital_transfers_site_measure, file = createMeasureFilename("hospital transfers", "site"), compress = "bzip2")
}
