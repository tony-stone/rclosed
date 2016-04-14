save_ed_attendances_measure <- function() {

  db_conn <- connect2DB()

  tbl_name <- "admissions_by_mode_trust_lsoa_month"

  add_field <- "aearrivalmode"


  # Prepare query string to create temp table
  sql_create_tbl <- getSqlUpdateQuery("ae", tbl_name, "", add_field)

  # Takes ~1.5mins
  pc <- proc.time()
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_tbl)
  proc.time() - pc

  # retrieve data
  ed_attendances_by_mode_trust_lsoa_month <- getDataFromTempTable(db_conn, tbl_name, add_field)

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  # save data
  save(ed_attendances_by_mode_trust_lsoa_month, file = "data/attendances by mode trust aetype lsoa month.Rda", compress = "xz")

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

  save(ed_attendances_by_mode_measure, file = "data/ed attendances by mode measure.Rda", compress = "bzip2")
}






save_unnecessary_ed_attendances_measure <- function() {

  db_conn <- connect2DB()

  tbl_name <- "admissions_without_treatment"

  field_indexes <- c(paste0("0", 1:9), 10:12)
  logic <- paste(paste0("(invest2_", field_indexes, " IS NULL OR substring(invest2_", field_indexes, " from 1 for 2) IN ('06', '6', '21', '22', '23', '24'))", collapse = " AND "), "AND",
    paste0("(treat2_", field_indexes, " IS NULL OR treat2_", field_indexes, " IN ('0', '00', '01', '1', '02', '2', '07', '7', '22', '30', '34', '56', '57', '99'))", collapse = " AND "), sep = " ")

  # Prepare query string to create temp table
  sql_create_tbl <- getSqlUpdateQuery("ae", tbl_name, logic)

  # Takes ~25s
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_tbl)

  # retrieve data
  unnecessary_ed_attendances_by_trust_lsoa_month <- getDataFromTempTable(db_conn, tbl_name)

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  # save data
  save(unnecessary_ed_attendances_by_trust_lsoa_month, file = "data/unnecessary ed attendances by trust lsoa month.Rda", compress = "xz")

  # Colapse by (lsoa, month)
  unnecessary_ed_attendances_by_lsoa_month <- unnecessary_ed_attendances_by_trust_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth)]

  # format
  unnecessary_ed_attendances_by_lsoa_month[, ':=' (measure = "unnecessary ed attendances",
    sub_measure = as.character(NA))]
  unnecessary_ed_attendances_measure <- fillDataPoints(unnecessary_ed_attendances_by_lsoa_month)

  # save measure
  save(unnecessary_ed_attendances_measure, file = "data/unnecessary ed attendances measure.Rda", compress = "xz")
}



getDistinctVals <- function(db_conn, field_name, indexes = "", src = "ae") {
  src_tbl <- ifelse(src == "ae", "relevant_ae_attendances", "relevent_apc_episodes")

  distinct_vals_list <- lapply(indexes, function(x) {
    return(DBI::dbGetQuery(db_conn, paste0("SELECT DISTINCT ", field_name, x, " FROM relevant_", src_tbl, "_attendances;")))
  })
  distinct_vals <- sort(unique(data.table::rbindlist(distinct_vals_list)[, paste0(field_name, indexes[1]), with = FALSE]), na.last = TRUE)

  return(distinct_vals)
}


getCounts <- function(db_conn, where = "1 = 1", src = "ae") {
  DBI::dbGetQuery(db_conn, paste0("SELECT COUNT(*) FROM relevant_", src, "_attendances WHERE ", where, ";"))
}

getOptimalCompress <- function(fname) {
  tools::resaveRdaFiles(fname)
  return(tools::checkRdaFiles(fname))
}

fillDataPoints <- function(data) {
  # Get catchment areas data
  load("data/catchment area set final.Rda")

  # Create datapoints for each lsoa/month/measure/sub_measure covering the measurement space (as this is not guaranteed from the data)
  data_points <- data.table::data.table(expand.grid(lsoa = unique(catchment_area_set_final$lsoa), yearmonth = seq(as.Date("2007-04-01"), as.Date("2014-03-01"), by = "month"), measure = unique(data$measure), sub_measure = unique(data$sub_measure), stringsAsFactors = FALSE))

  # Merge data into data points so we have a record/row for each and every point
  data_all_points <- merge(data_points, data, by = c("lsoa", "yearmonth", "measure", "sub_measure"), all = TRUE)
  # if we do not have a value for a data point within the period for which we have data, set this to 0
  # (else, if outside period for which we have data, it will remain as NA)
  data_all_points[is.na(value) & yearmonth >= min(data$yearmonth) & yearmonth <= max(data$yearmonth), value := 0]

  # Merge in catchment area data
  data_measure <- merge(data_all_points, catchment_area_set_final, by = "lsoa", all = TRUE)

  # Prepare time_to_ed field/variable and finalise dataset
  data_measure[, time_to_ed := time_to_ae_post_intv]
  data_measure[yearmonth < intervention_date, time_to_ed := time_to_ae_pre_intv]
  data_measure[, c("time_to_ae_pre_intv", "time_to_ae_post_intv", "intervention_date") := NULL]

  return(data_measure)
}






connect2DB <- function() {
  load(file = "data/DB settings read.rda")

  # Form the URL from the above set of params
  db_url <- paste0("jdbc:postgresql://", db_config["host"], ":", db_config["port"], "/", db_config["db_name"], "?user=", db_config["user"], "&defaultAutoCommit")

  # Specify the driver, the Java driver (JAR) file, and that Postgres uses double quotes (") to escape identifiers (keywords)
  db_drvr <- RJDBC::JDBC("org.postgresql.Driver", "C:/Program Files/PostgreSQL/postgresql-9.4.1207.jar", "\"")

  # Set up our DB connection
  return(DBI::dbConnect(db_drvr, db_url, password = db_config["pass"]))
}






getDataFromTempTable <- function(conn, name, other_fields = "") {

  std_fields <- "procode3, lsoa01, aedepttype, yearmonth, value"
  other_fields <- ifelse(other_fields == "", "", paste0(", ", other_fields))

  # Get size of temp table
  nrows <- DBI::dbGetQuery(conn, paste0("SELECT COUNT(*) FROM ", name, ";"))[1, 1]

  # Set offset and limit var
  offset <- 0L
  limit <- 500000L
  sql_select <- paste0("SELECT ", std_fields, other_fields, " FROM ", name)

  sql_query_select <- paste0(sql_select, " WHERE row > ", offset, " AND row <= ", offset + limit, ";")
  df_data <- DBI::dbGetQuery(conn, sql_query_select)

  # Need to call gc() to clear up Java heap space
  gc()

  offset <- offset + limit

  # Loop to fetch all rows of data (takes ~45s in entirety)
  while (offset < nrows) {
    sql_query_select <- paste0(sql_select, " WHERE row > ", offset, " AND row <= ", offset + limit, ";")
    df_data <- rbind(df_data, DBI::dbGetQuery(conn, sql_query_select))
    gc()
    offset <- offset + limit
  }

  # Convert to data.table
  data <- data.table::data.table(df_data)

  # Convert dates to date type
  data[, yearmonth := as.Date(lubridate::fast_strptime(paste0(yearmonth, "-01"), format = "%Y-%m-%d"))]

  # Standardise field names
  data.table::setnames(data, "lsoa01", "lsoa")

  return(data)
}


getSqlUpdateQuery <- function(src = "ae", temp_tbl, select_logic = "", other_fields = "") {

  src_tbl <- ifelse(src == "ae", "relevant_ae_attendances", "relevent_apc_episodes")

  std_fields <- "procode3, lsoa01, aedepttype, yearmonth, value"
  std_fields_constr <- c("procode3, lsoa01, aedepttype, substring(arrivaldate from 1 for 7)", " AS yearmonth, COUNT(*) AS value")
  sql_fields_units_std <- c(std_fields, paste0(std_fields_constr, collapse = ""), std_fields_constr[1])

  additional_fields <- ifelse(other_fields == "", "", paste0(", ", other_fields))
  sql_fields_units <- paste0(sql_fields_units_std, additional_fields)

  additional_logic <- ifelse(select_logic == "", "", paste0("AND ", select_logic))

  # Prepare query string to create temp table
  sql <- paste("CREATE TEMP TABLE", temp_tbl, "AS SELECT row_number() OVER () AS row,", sql_fields_units[1], "FROM (",
    "SELECT", sql_fields_units[2], "FROM", src_tbl, "wHERE aeattendcat = '1' AND (aedepttype = '01' OR aedepttype = '99') AND",
    "(aeattenddisp = '02' OR aeattenddisp = '03' OR aeattenddisp = '12')",
    additional_logic,
    "GROUP BY", sql_fields_units[3], ") AS st;", sep = " ")

  return(sql)
}
