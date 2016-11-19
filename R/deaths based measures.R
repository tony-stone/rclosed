save_case_fatality_measure <- function(days_to_death_cuts = c(3, 7, 30)) {

  load("data/hes apc cips and deaths.Rda")

  invisible(
  case_fatality_measure_list <- lapply(days_to_death_cuts, function(days, data){
    deaths_lsoa <- prepareDeathsMeasure(days, data[death_record == TRUE])
    cases_lsoa <- prepareCases(days, data[valid_as_case_only == TRUE])[, c("measure", "group", "site_type", "relative_month", "diff_time_to_ed") := NULL]

    case_fatality <- merge(deaths_lsoa, cases_lsoa, by = c("lsoa", "yearmonth", "sub_measure", "town"), all = TRUE)
    case_fatality_any <- case_fatality[sub_measure != "any sec" & sub_measure != "other", .(value.x = sum(value.x), value.y = sum(value.y)), by = .(lsoa, yearmonth, town, measure, group, site_type, relative_month, diff_time_to_ed)]
    case_fatality_any[, sub_measure := "any single sec"]

    case_fatality_all <- rbind(case_fatality, case_fatality_any)
    case_fatality_all[, measure := paste0("sec case fatality ", days, " days")]

    case_fatality_measure <- copy(case_fatality_all)

    case_fatality_measure[, ':=' (value = value.x / (value.x + value.y),
      value.x = NULL,
      value.y = NULL)]

    case_fatality_site_measure <- case_fatality_all[, .(value.x = sum(value.x), value.y = sum(value.y)), by = .(yearmonth, town, measure, sub_measure, group, site_type, relative_month)]
    case_fatality_site_measure[, ':=' (value = value.x / (value.x + value.y),
      value.x = NULL,
      value.y = NULL)]

    vname <- paste0("sec_case_fatality_", days, "days_measure")
    assign(vname, case_fatality_measure)
    save(list = vname, file = createMeasureFilename(paste0("sec case fatality ", days, "days"), "lsoa"))

    vname <- paste0("sec_case_fatality_", days, "days_site_measure")
    assign(vname, case_fatality_site_measure)
    save(list = vname, file = createMeasureFilename(paste0("sec case fatality ", days, "days"), "site"))

  }, data = hes_apc_cips_and_deaths))
}


save_deaths_measure <- function(days_to_death_cuts = c(3, 7, 30)) {

  load("data/hes apc cips and deaths.Rda")

  hes_apc_cips_with_deaths <- hes_apc_cips_and_deaths[death_record == TRUE]

  deaths_measures_lsoa <- lapply(days_to_death_cuts, prepareDeathsMeasure, deaths_data = hes_apc_cips_with_deaths)

  invisible(
    lapply(deaths_measures_lsoa, function(data) {
    vname <- unique(data$measure)
    assign(paste0(vname, "_measure"), data)
    save(list = paste0(vname, "_measure"), file = createMeasureFilename(gsub("_", " ", vname, fixed = TRUE), "lsoa"))
  }))

  deaths_measures_site <- lapply(deaths_measures_lsoa, collapseLsoas2Sites)

  invisible(
  lapply(deaths_measures_site, function(data) {
    vname <- unique(data$measure)
    assign(paste0(vname, "_site_measure"), data)
    save(list = paste0(vname, "_site_measure"), file = createMeasureFilename(gsub("_", " ", vname, fixed = TRUE), "site"))
  }))
}


prepareDeathsMeasure <- function(days, deaths_data) {
  deaths <- copy(deaths_data[died_in_cips == FALSE | (died_in_cips == TRUE & days_to_death_grp <= days)])

  deaths[died_in_cips == TRUE, ':=' (lsoa = lsoa_case,
    yearmonth = yearmonth_case)]
  deaths[died_in_cips == FALSE, ':=' (lsoa = lsoa_death,
    yearmonth = yearmonth_death)]

  setnames(deaths, "condition_death", "condition")

  deaths_measure_by_condition <- deaths[substr(lsoa, 1, 1) == "E", .(value = .N), by = .(lsoa, yearmonth, condition)]
  deaths_measure_all <- deaths_measure_by_condition[condition != "other", .(value = sum(value)), by = .(lsoa, yearmonth)]

  setnames(deaths_measure_by_condition, "condition", "sub_measure")
  deaths_measure_all[, sub_measure := "any sec"]

  deaths_measure <- rbind(deaths_measure_by_condition, deaths_measure_all)
  deaths_measure[, measure := paste0("sec_deaths_", days, "days")]

  deaths_measure <- fillDataPoints(deaths_measure)

  return(deaths_measure)
}


prepareCases <- function(days, cases_data) {
  cases <- copy(cases_data[(died_in_cips == TRUE & days_to_death_grp > days) | is.na(days_to_death_grp)])

  setnames(cases, c("lsoa_case", "yearmonth_case", "condition_case"), c("lsoa", "yearmonth", "condition"))

  cases_measure_by_condition <- cases[, .(value = .N), by = .(lsoa, yearmonth, condition)]
  cases_measure_by_condition_all <- cases_measure_by_condition[condition != "other", .(value = sum(value)), by = .(lsoa, yearmonth)]

  setnames(cases_measure_by_condition, "condition", "sub_measure")
  cases_measure_by_condition_all[, sub_measure := "any sec"]

  cases_measure <- rbind(cases_measure_by_condition, cases_measure_by_condition_all)
  cases_measure[, measure := paste0("sec_case_fatality_", days, "days")]

  cases_measure <- fillDataPoints(cases_measure)

  return(cases_measure)
}


link_hes_apc_and_death_data <- function(days_to_death_cuts = c(3, 7, 30)) {
  db_conn <- connect2DB()

  # Prepare query string to create temp table
  sql_create_tbl <- "CREATE TEMP TABLE individual_cips AS SELECT row_number() OVER () AS row, encrypted_hesid, lsoa01, cips_start, cips_end, diag_01, diag_02, cause, startage, nights_admitted FROM relevant_apc_cips_data WHERE emergency_admission = TRUE AND cips_finished = TRUE AND nights_admitted < 184;"

  # Calc data
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_tbl)

  # retrieve data
  # Get size of temp table
  nrows <- DBI::dbGetQuery(db_conn, paste0("SELECT COUNT(*) FROM individual_cips;"))[1, 1]

  # Set offset and limit var
  offset <- 0L
  limit <- 300000L
  sql_select <- paste0("SELECT encrypted_hesid, lsoa01, cips_start, cips_end, diag_01, diag_02, cause, startage, nights_admitted FROM individual_cips")

  sql_query_select <- paste0(sql_select, " WHERE row > ", offset, " AND row <= ", offset + limit, ";")
  df_data <- DBI::dbGetQuery(db_conn, sql_query_select)

  # Need to call gc() to clear up Java heap space
  gc()

  offset <- offset + limit

  while (offset < nrows) {
    sql_query_select <- paste0(sql_select, " WHERE row > ", offset, " AND row <= ", offset + limit, ";")
    df_data <- rbind(df_data, DBI::dbGetQuery(db_conn, sql_query_select))
    gc()
    offset <- offset + limit
  }

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL


  # Convert to data.table
  hes_apc_cips <- data.table(df_data)

  rm(df_data)
  gc()


  # classify condition
  hes_apc_cips <- classifyAvoidableDeaths(hes_apc_cips)
  hes_apc_cips[, startage := NULL]

  # convert dates
  hes_apc_cips[, ':=' (yearmonth = as.Date(lubridate::fast_strptime(paste0(substr(cips_start, 1, 7), "-01"), format = "%Y-%m-%d", lt = FALSE)),
    cips_start = as.Date(lubridate::fast_strptime(cips_start, format = "%Y-%m-%d", lt = FALSE)),
    cips_end = as.Date(lubridate::fast_strptime(cips_end, format = "%Y-%m-%d", lt = FALSE)))]

  # Standardise field names
  setnames(hes_apc_cips, c("lsoa01", "yearmonth", "condition"), c("lsoa_case", "yearmonth_case", "condition_case"))

  # Load death data
  load("data/hes linked mortality.Rda")

  ## Make long form and classify causes of deaths as one of the SECs
  ons_deaths_long <- melt(HES_linked_mortality, id.vars = c("encrypted_hesid", "startage", "sex", "date_of_death", "lsoa"), na.rm = TRUE, variable.factor = FALSE, variable.name = "condition_rank", value.name = "diag_01")
  ons_deaths_long[, ':=' (diag_02 = NA,
    cause = diag_01,
    condition_rank = as.integer(substr(condition_rank, 12, 13)))]

  ons_deaths_long <- classifyAvoidableDeaths(ons_deaths_long)

  ons_deaths_long[condition %in% c("road traffic accident", "falls", "self harm"), condition_rank := condition_rank + 16L]
  ons_deaths_long[condition == "other", condition_rank := condition_rank + 32L]

  ons_deaths <- ons_deaths_long[, min_rank := min(condition_rank), by = encrypted_hesid][condition_rank == min_rank, .(encrypted_hesid, date_of_death, lsoa, condition)]

  ons_deaths[, ':=' (yearmonth_death = as.Date(lubridate::fast_strptime(paste0(substr(date_of_death, 1, 7), "-01"), format = "%Y-%m-%d", lt = FALSE)),
    date_of_death = as.Date(lubridate::fast_strptime(date_of_death, format = "%Y-%m-%d", lt = FALSE)))]

  setnames(ons_deaths, c("lsoa", "condition"), c("lsoa_death", "condition_death"))

  rm(ons_deaths_long, HES_linked_mortality)

  # Merge APC CIPS and death data, keeping all data
  hes_apc_cips_and_deaths <- merge(hes_apc_cips, ons_deaths, by = "encrypted_hesid", all = TRUE)

  # For those with >1 APC CIPS, rank by: 1) patient; 2) latest finishing CIPS; 3) earliest starting CIPS
  hes_apc_cips_and_deaths[!is.na(cips_start) & !is.na(date_of_death), cips_rank := frankv(hes_apc_cips_and_deaths[!is.na(cips_start) & !is.na(date_of_death)], cols = c("encrypted_hesid", "cips_end", "cips_start"), order = c(1, -1, 1), ties.method = "random")]
  hes_apc_cips_and_deaths[!is.na(cips_rank), cips_rank_patient := rank(cips_rank, ties.method = "min"), by = encrypted_hesid]
  stopifnot(hes_apc_cips_and_deaths[!is.na(cips_rank_patient), .N, by = .(encrypted_hesid, cips_rank_patient)][N > 1, .N] == 0) # there should be no ties

  # Mark only the latest finishing, earliest starting CIPS or the single death record (for those with no APC CIPS)
  hes_apc_cips_and_deaths[, death_record := FALSE]
  hes_apc_cips_and_deaths[cips_rank_patient == 1 | (is.na(cips_rank_patient) & !is.na(date_of_death)), death_record := TRUE]

  # Ignoring death records (mark these seperately), only keep CIPS where the patient was admitted for at least 2 nights
  hes_apc_cips_and_deaths <- hes_apc_cips_and_deaths[death_record == TRUE | (death_record == FALSE & nights_admitted > 1)]

  # classify days to death
  hes_apc_cips_and_deaths[, days_to_death_grp := c(days_to_death_cuts, Inf)[cut(
    as.integer(date_of_death - cips_start),
    c(0, days_to_death_cuts - 1, Inf),
    include.lowest = TRUE,
    labels = FALSE)]]

  # For CIPS attached to the death record decide if the CIPS would be valid as a "case"
  # i.e. check: admited > 1 night and check the patient had not "already died" (we trust the death record over the HES data))
  hes_apc_cips_and_deaths[, valid_as_case_only := (!is.na(nights_admitted) & !is.na(days_to_death_grp) & nights_admitted > 1)]

  # mark CIPS in which the patient dies - not necessarily the death record
  hes_apc_cips_and_deaths[, died_in_cips := (!is.na(cips_end) & !is.na(date_of_death) & !is.na(days_to_death_grp) & cips_end >= date_of_death)]

  # remove fields we don't require
  hes_apc_cips_and_deaths[, c("cips_start",
    "cips_end",
    "cips_rank",
    "cips_rank_patient",
    "date_of_death",
    "nights_admitted") := NULL]

  # ensure we have the same number of deaths as we started with
  stopifnot(hes_apc_cips_and_deaths[death_record == TRUE, .N] == ons_deaths[, .N])

  save(hes_apc_cips_and_deaths, file = "data/hes apc cips and deaths.Rda", compress = "xz")
}
