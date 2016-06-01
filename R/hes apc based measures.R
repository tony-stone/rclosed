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

  ## Deal with error codes in the diag_01 field
  # cause code in primary diagnosis field
  emergency_admissions_by_diagnosis_site_lsoa_month[diag_01 == "R69X3", diag_01 := diag_02]

  ## categorise into "avoidable admissions" conditions
  # default to 'other'
  emergency_admissions_by_diagnosis_site_lsoa_month[, sub_measure := "other"]

  # Create 1 character, 3 character and 4 character codes diagnosis codes and 3 character cause code
  emergency_admissions_by_diagnosis_site_lsoa_month[, diag_4char := toupper(substr(diag_01, 1, 4))]
  emergency_admissions_by_diagnosis_site_lsoa_month[, diag_3char := substr(diag_4char, 1, 3)]
  emergency_admissions_by_diagnosis_site_lsoa_month[, diag_1char := substr(diag_4char, 1, 1)]
  emergency_admissions_by_diagnosis_site_lsoa_month[, diag_cause := toupper(substr(cause, 1, 3))]

  # Hypoglycemia
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & (diag_3char %in% paste0("E", 10:15) | diag_4char %in% paste0("E", 161:162)), sub_measure := "hypoglycaemia"]

  # Acute mental health crisis
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_1char == "F", sub_measure := "acute mental health crisis"]

  # Epileptic fit
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_3char %in% paste0("G", 40:41), sub_measure := "epileptic fit"]

  # Angina
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_3char == "I20", sub_measure := "angina"]

  # DVT
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_3char %in% paste0("I", 80:82), sub_measure := "dvt"]

  # COPD
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_3char %in% paste0("J", 40:44), sub_measure := "copd"]

  # Cellulitis
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_3char == "L03", sub_measure := "cellulitis"]

  # Urinary tract infection
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_4char == "N390", sub_measure := "urinary tract infection"]

  # Non-specific chest pain
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_4char %in% paste0("R0", 72:74), sub_measure := "non-specific chest pain"]

  # Non-specific abdominal pain
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_3char == "R10", sub_measure := "non-specific abdominal pain"]

  # Pyrexial child (<6 years)
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_3char == "R50" & startage < 6L, sub_measure := "pyrexial child (<6 years)"]

  # Minor head injuries
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_3char == "S00", sub_measure := "minor head injuries"]

  # Blocked catheter
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_4char == "T830", sub_measure := "blocked catheter"]

  # Falls (75+ years)
  falls_codes_digits <- expand.grid(d1 = 0:1, d2 = 0:9, stringsAsFactors = FALSE)
  falls_codes <- paste0("W", falls_codes_digits$d1, falls_codes_digits$d2)
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_cause %in% falls_codes & startage >= 75L, sub_measure := "falls (75+ years)"]

  # collapse to attendances by (lsoa, month, avoidable condition)
  emergency_admissions_by_condition_lsoa_month <- emergency_admissions_by_diagnosis_site_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth, sub_measure)]

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




save_case_fatality_measure <- function() {

  db_conn <- connect2DB()

  tbl_name_1 <- "cips_by_diagnosis_site_lsoa_month"
  add_logic_1 <- "cips_finished = TRUE AND nights_admitted < 184"
  add_fields_1 <- "diag_01, diag_02, cause, startage"

  tbl_name_2 <- "deaths_by_diagnosis_site_lsoa_month"
  add_logic_2 <- "cips_finished = TRUE AND nights_admitted < 3 AND died = TRUE AND date_of_death_last = last_disdate"
  add_fields_2 <- "diag_01, diag_02, cause, startage"

  # Prepare query string to create temp table
  sql_create_tbl_1 <- getSqlUpdateQuery("apc", tbl_name_1, add_logic_1, add_fields_1)
  sql_create_tbl_2 <- getSqlUpdateQuery("apc", tbl_name_2, add_logic_2, add_fields_2)

  # Takes ~40s
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_tbl_1)
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_tbl_2)

  # retrieve data, takes ~20s
  hes_apc_cips <- getDataFromTempTable(db_conn, tbl_name_1, "apc", add_fields_1)
  hes_apc_deaths <- getDataFromTempTable(db_conn, tbl_name_2, "apc", add_fields_2)

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  # classify conditions, ~5s
  hes_apc_cips <- classifyAvoidableDeaths(hes_apc_cips)
  hes_apc_deaths <- classifyAvoidableDeaths(hes_apc_deaths)

  # aggregate to (lsoa, month, avoidable condition)
  hes_apc_cips_aggregated <- hes_apc_cips[condition != "other", .(value = sum(value)), by = .(lsoa, yearmonth, condition)]
  hes_apc_deaths_aggregated <- hes_apc_deaths[condition != "other", .(fatalities = sum(value)), by = .(lsoa, yearmonth, condition)]

  # format
  setnames(hes_apc_cips_aggregated, "condition", "sub_measure")
  setnames(hes_apc_deaths_aggregated, "condition", "sub_measure")
  hes_apc_cips_aggregated[, measure := "case fatality ratio"]
  hes_apc_cips_aggregated <- fillDataPoints(hes_apc_cips_aggregated, FALSE) # Do not treat as count data (i.e. maintain NAs)

  # merge cips and 3-day deaths
  case_fatality_single_measure <- merge(hes_apc_cips_aggregated, hes_apc_deaths_aggregated, by = c("lsoa", "yearmonth", "sub_measure"), all.x = TRUE)

  # for those where there are no fatalities (NA - missing data) set fatalities to 0
  case_fatality_single_measure[is.na(fatalities) & !is.na(value), fatalities := 0]

  # compute cases and fatalities by any condition
  case_fatality_any_measure <- case_fatality_single_measure[, .(value = sum(value, na.rm = TRUE), fatalities = sum(fatalities, na.rm = TRUE)), by = .(lsoa, yearmonth, measure, town, group, site_type, relative_month, diff_time_to_ed)]
  case_fatality_any_measure[value == 0, ':=' (value = NA,
    fatalities = NA)]
  case_fatality_any_measure[, sub_measure := "any"]

  # compute case fatality ratio
  case_fatality_measure <- rbind(case_fatality_single_measure, case_fatality_any_measure)
  case_fatality_site_measure <- case_fatality_measure[, .(value = sum(value, na.rm = TRUE), fatalities = sum(fatalities, na.rm = TRUE)), by = .(yearmonth, measure, sub_measure, town, group, site_type, relative_month)]
  case_fatality_site_measure[value == 0, ':=' (value = NA,
    fatalities = NA)]

  case_fatality_measure[, ':=' (value = fatalities / value,
    fatalities = NULL)]

  case_fatality_site_measure[, ':=' (value = fatalities / value,
    fatalities = NULL)]

  # save
  save(case_fatality_measure, file = createMeasureFilename("case fatality"), compress = "xz")
  save(case_fatality_site_measure, file = createMeasureFilename("case fatality", "site"), compress = "bzip2")
}
