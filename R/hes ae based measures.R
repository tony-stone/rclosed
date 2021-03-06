save_ed_attendances_measure <- function() {

  db_conn <- connect2DB()

  tbl_name <- "attendances_by_mode_trust_lsoa_month"
  add_logic <- "aeattendcat = '1'"
  add_fields <- "aearrivalmode"

  # Prepare query string to create temp table
  sql_create_tbl <- getSqlUpdateQuery("ae", tbl_name, add_logic, add_fields)

  # Takes ~1.5mins
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_tbl)

  # retrieve data
  ed_attendances_by_mode_trust_lsoa_month <- getDataFromTempTable(db_conn, tbl_name, "ae", add_fields)

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  # collapse to attendances by (lsoa, month, mode of arrival)
  ed_attendances_by_mode_lsoa_month <- ed_attendances_by_mode_trust_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth, aearrivalmode)]

  # any mode of arrival
  attendances_by_lsoa_month <- ed_attendances_by_mode_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth)]
  attendances_by_lsoa_month[, aearrivalmode := "any"]

  # sub-categories of mode of arrival
  ed_attendances_by_mode_lsoa_month[aearrivalmode == "1", aearrivalmode := "ambulance"]
  ed_attendances_by_mode_lsoa_month[aearrivalmode == "2", aearrivalmode := "other"]

  # combine
  ed_attendances_by_mode_lsoa_month <- rbind(attendances_by_lsoa_month, ed_attendances_by_mode_lsoa_month)

  # remove "unknown" mode of arrival
  ed_attendances_by_mode_lsoa_month <- ed_attendances_by_mode_lsoa_month[aearrivalmode == "ambulance" | aearrivalmode == "other" | aearrivalmode == "any"]

  # format
  data.table::setnames(ed_attendances_by_mode_lsoa_month, "aearrivalmode", "sub_measure")
  ed_attendances_by_mode_lsoa_month[, measure := "ed attendances"]

  ed_attendances_by_mode_measure <- fillDataPoints(ed_attendances_by_mode_lsoa_month)

  ed_attendances_by_mode_site_measure <- collapseLsoas2Sites(ed_attendances_by_mode_measure)

  save(ed_attendances_by_mode_measure, file = createMeasureFilename("ed attendances by mode"), compress = "bzip2")
  save(ed_attendances_by_mode_site_measure, file = createMeasureFilename("ed attendances by mode", "site"), compress = "bzip2")
}



save_unnecessary_ed_attendances_measure <- function() {

  db_conn <- connect2DB()

  tbl_name <- "attendances_without_treatment"
  field_indexes <- c(paste0("0", 1:9), 10:12)
  add_logic <- paste("aeattendcat = '1' AND",
    "(aeattenddisp = '02' OR aeattenddisp = '03' OR aeattenddisp = '12') AND",
    paste0("(invest2_", field_indexes, " IS NULL OR substring(invest2_", field_indexes, " from 1 for 2) IN ('06', '6', '21', '22', '23', '24'))", collapse = " AND "), "AND",
    paste0("(treat2_", field_indexes, " IS NULL OR treat2_", field_indexes, " IN ('0', '00', '01', '1', '02', '2', '07', '7', '22', '30', '34', '56', '57', '99'))", collapse = " AND "), sep = " ")
  add_fields <- ""

  # Prepare query string to create temp table
  sql_create_tbl <- getSqlUpdateQuery("ae", tbl_name, add_logic, add_fields)

  # Takes ~25s
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_tbl)

  # retrieve data
  unnecessary_ed_attendances_by_trust_lsoa_month <- getDataFromTempTable(db_conn, tbl_name, "ae", "")

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  # Colapse by (lsoa, month)
  unnecessary_ed_attendances_by_lsoa_month <- unnecessary_ed_attendances_by_trust_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth)]

  # format
  unnecessary_ed_attendances_by_lsoa_month[, ':=' (measure = "unnecessary ed attendances",
    sub_measure = as.character(NA))]

  unnecessary_ed_attendances_measure <- fillDataPoints(unnecessary_ed_attendances_by_lsoa_month)

  unnecessary_ed_attendances_site_measure <- collapseLsoas2Sites(unnecessary_ed_attendances_measure)

  # save measure
  save(unnecessary_ed_attendances_measure, file = createMeasureFilename("unnecessary ed attendances"), compress = "xz")
  save(unnecessary_ed_attendances_site_measure, file = createMeasureFilename("unnecessary ed attendances", "site"), compress = "bzip2")
}



save_ed_attendances_admitted_measure <- function() {

  db_conn <- connect2DB()

  tbl_name <- "attendances_admitted"
  add_logic <- "aeattendcat = '1'"
  add_fields <- "aeattenddisp"

  # Prepare query string to create temp table
  sql_create_tbl <- getSqlUpdateQuery("ae", tbl_name, add_logic, add_fields)

  # Takes ~3.5mins
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_tbl)

  # retrieve data
  ed_attendances_by_disposal_trust_lsoa_month <- getDataFromTempTable(db_conn, tbl_name, "ae", "aeattenddisp")

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  # Collapse by (lsoa, month, aeattenddisp)
  ed_attendances_by_disposal_lsoa_month <- ed_attendances_by_disposal_trust_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth, aeattenddisp)]
  data.table::setnames(ed_attendances_by_disposal_lsoa_month, "aeattenddisp", "sub_measure")

  # All attendances
  ed_attendances_by_lsoa_month <- ed_attendances_by_disposal_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth)]
  ed_attendances_by_lsoa_month[, sub_measure := "all"]

  # Attendances that are admitted
  ed_attendances_admitted_by_lsoa_month <- ed_attendances_by_disposal_lsoa_month[sub_measure == "01"]
  ed_attendances_admitted_by_lsoa_month[, sub_measure := "admitted"]

  # combine
  ed_attendances_by_lsoa_month <- rbind(ed_attendances_by_lsoa_month, ed_attendances_admitted_by_lsoa_month)
  ed_attendances_by_lsoa_month[, measure := "ed attendances admitted"]

  ed_attendances_admitted_measure <- fillDataPoints(ed_attendances_by_lsoa_month)
  ed_attendances_admitted_site_measure <- collapseLsoas2Sites(ed_attendances_admitted_measure)

  ed_attendances_admitted_measure <- addFractionSubmeasure(ed_attendances_admitted_measure, "all", "admitted", "fraction admitted")
  ed_attendances_admitted_site_measure <- addFractionSubmeasure(ed_attendances_admitted_site_measure, "all", "admitted", "fraction admitted")

  # save measure
  save(ed_attendances_admitted_measure, file = createMeasureFilename("ed attendances admitted"), compress = "xz")
  save(ed_attendances_admitted_site_measure, file = createMeasureFilename("ed attendances admitted", "site"), compress = "xz")
}

