save_case_fatality_measure <- function() {

  db_conn <- connect2DB()

  tbl_name <- "cips_by_diagnosis_site_lsoa_month"
  add_logic <- "cips_finished = TRUE AND nights_admitted > 1 AND nights_admitted < 184 AND died = FALSE"
  add_fields <- "diag_01, diag_02, cause, startage"

  # Prepare query string to create temp table
  sql_create_tbl <- getSqlUpdateQuery("apc", tbl_name, add_logic, add_fields)

  # Calc data
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_tbl)

  # retrieve data
  hes_apc_cips <- getDataFromTempTable(db_conn, tbl_name, "apc", add_fields)

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  # classify conditions, ~5s
  hes_apc_cips <- classifyAvoidableDeaths(hes_apc_cips)

  # aggregate CIPS to (lsoa, month, avoidable condition)
  hes_apc_cips_by_condition <- hes_apc_cips[condition != "other", .(non_fatal_cases = sum(value)), by = .(lsoa, yearmonth, condition)]

  # load death data
  load("data/ons deaths (16 serious emergency conditions).Rda")
  deaths_by_condition <- deaths_serious_emergency_conditions[, .(fatalities = sum(deaths)), by = .(lsoa, yearmonth, cause_of_death)]
  setnames(deaths_by_condition, "cause_of_death", "condition")

  case_fatality_by_condition <- merge(deaths_by_condition, hes_apc_cips_by_condition, by = c("lsoa", "yearmonth", "condition"), all = TRUE)
  case_fatality_by_condition[is.na(non_fatal_cases), non_fatal_cases := 0L]
  case_fatality_by_condition[is.na(fatalities), fatalities := 0L]
  case_fatality_by_condition[, ':=' (cases = non_fatal_cases + fatalities,
    non_fatal_cases = NULL)]
  case_fatality_all_conditions <- case_fatality_by_condition[, .(cases = sum(cases), fatalities = sum(fatalities)), by = .(lsoa, yearmonth)]

  # format
  setnames(case_fatality_by_condition, "condition", "sub_measure")
  case_fatality_all_conditions[, sub_measure := "any"]

  # combine
  case_fatality <- rbind(case_fatality_by_condition, case_fatality_all_conditions)
  case_fatality[, measure := "case fatality ratio"]

  # calc site level
  case_fatality_with_site<- attachTownForSiteLevelData(case_fatality)
  case_fatality_site <- case_fatality_with_site[, .(cases = sum(cases), fatalities = sum(fatalities)), by = .(town, yearmonth, measure, sub_measure)]

  # calc ratios
  case_fatality[, ':=' (value = fatalities / cases,
    cases = NULL,
    fatalities = NULL)]

  case_fatality_site[, ':=' (value = fatalities / cases,
    cases = NULL,
    fatalities = NULL)]

  # format -  Do not treat as count data (i.e. maintain NAs)
  case_fatality_measure <- fillDataPoints(case_fatality, FALSE)
  case_fatality_site_measure <- fillDataPoints(case_fatality_site, FALSE, FALSE)

  # save
  save(case_fatality_measure, file = createMeasureFilename("case fatality"), compress = "xz")
  save(case_fatality_site_measure, file = createMeasureFilename("case fatality", "site"), compress = "bzip2")
}
