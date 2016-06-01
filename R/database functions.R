connect2DB <- function() {
  load(file = "data/DB settings read.rda")

  # Form the URL from the above set of params
  db_url <- paste0("jdbc:postgresql://", db_config["host"], ":", db_config["port"], "/", db_config["db_name"], "?user=", db_config["user"], "&defaultAutoCommit")

  # Specify the driver, the Java driver (JAR) file, and that Postgres uses double quotes (") to escape identifiers (keywords)
  db_drvr <- RJDBC::JDBC("org.postgresql.Driver", "C:/Program Files/PostgreSQL/postgresql-9.4.1207.jar", "\"")

  # Set up our DB connection
  return(DBI::dbConnect(db_drvr, db_url, password = db_config["pass"]))
}




getDBTableName <- function(shorthand) {
  if(missing(shorthand) | is.na(shorthand)) stop("Undefined source table name.")
  shorthand <- tolower(shorthand)
  if(shorthand == "ae") {
    rt_val <- "relevant_ae_attendances"
  } else if(shorthand == "apc") {
    rt_val <- "relevant_apc_cips_data"
  } else {
    stop("Undefined source table name.")
  }
  return(rt_val)
}




deleteDBTable <- function(db_conn, table) {
  if(RJDBC::dbExistsTable(db_conn, table) == TRUE) {
    tryCatch(invisible(RJDBC::dbRemoveTable(db_conn, table)), error = function(e) { stop(paste0("Could not remove DB table: ", table)) }, finally = NULL)
  }
}




getDistinctVals <- function(db_conn, field_name, indexes = "", src = "ae") {
  src_tbl <- getDBTableName(src)

  distinct_vals_list <- lapply(indexes, function(x) {
    return(DBI::dbGetQuery(db_conn, paste0("SELECT ", field_name, ", COUNT(*) AS n FROM ", src_tbl, " GROUP BY ", field_name, ";")))
  })

  distinct_vals <- data.table::rbindlist(distinct_vals_list)
  data.table::setnames(distinct_vals, c("value", "n"))
  distinct_vals <- distinct_vals[, .(n = sum(n)), by = value]
  data.table::setorder(distinct_vals, value)
  return(distinct_vals)
}




getCounts <- function(db_conn, where = "1 = 1", src = "ae") {
  src_tbl <- getDBTableName(src)
  DBI::dbGetQuery(db_conn, paste0("SELECT COUNT(*) FROM ", src_tbl, " WHERE ", where, ";"))
}




getDataFromTempTable <- function(conn, name, src = "ae", other_fields = "") {
  # Much of this is a work around in order to get a large volume of data from PostgreSQL in (memory) manageable chunks

  std_fields <- ifelse(src == "ae", "procode3, lsoa01, aedepttype, yearmonth, value", "procode, lsoa01, yearmonth, value")
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

  while (offset < nrows) {
    sql_query_select <- paste0(sql_select, " WHERE row > ", offset, " AND row <= ", offset + limit, ";")
    df_data <- rbind(df_data, DBI::dbGetQuery(conn, sql_query_select))
    gc()
    offset <- offset + limit
  }

  # Convert to data.table
  data <- data.table::data.table(df_data)

  # Standardise field types
  if(src == "ae") {
    data[, yearmonth := as.Date(lubridate::fast_strptime(paste0(yearmonth, "-01"), format = "%Y-%m-%d", lt = FALSE))]
  } else {
    data[, yearmonth := as.Date(lubridate::fast_strptime(yearmonth, format = "%Y-%m-%d", lt = FALSE))]
  }

  # Standardise field names
  data.table::setnames(data, "lsoa01", "lsoa")

  return(data)
}




getSqlUpdateQuery <- function(src = "ae", temp_tbl, select_logic = "", other_fields = "") {

  src_tbl <- getDBTableName(src)

  if(src_tbl == "relevant_ae_attendances") {
    std_fields <- "procode3, lsoa01, aedepttype, yearmonth, value"
    std_fields_constr <- c("procode3, lsoa01, aedepttype, substring(arrivaldate from 1 for 7)", " AS yearmonth, COUNT(*) AS value")
    sql_fields_units_std <- c(std_fields, paste0(std_fields_constr, collapse = ""), std_fields_constr[1])

    additional_fields <- ifelse(other_fields == "", "", paste0(", ", other_fields))
    sql_fields_units <- paste0(sql_fields_units_std, additional_fields)

    additional_logic <- ifelse(select_logic == "", "", paste0("AND ", select_logic))

    # Prepare query string to create temp table
    sql <- paste("CREATE TEMP TABLE", temp_tbl, "AS SELECT row_number() OVER () AS row,", sql_fields_units[1], "FROM (",
      "SELECT", sql_fields_units[2], "FROM", src_tbl, "wHERE aeattendcat = '1'",
      additional_logic,
      "GROUP BY", sql_fields_units[3], ") AS st;")

  } else {

    std_fields <- "procode, lsoa01, yearmonth, value"
    std_fields_constr <- c("procode, lsoa01,  to_char(date_trunc('month', cips_start), 'YYYY-MM-DD')", " AS yearmonth, COUNT(*) AS value")
    sql_fields_units_std <- c(std_fields, paste0(std_fields_constr, collapse = ""), std_fields_constr[1])

    additional_fields <- ifelse(other_fields == "", "", paste0(", ", other_fields))
    sql_fields_units <- paste0(sql_fields_units_std, additional_fields)

    additional_logic <- ifelse(select_logic == "", "", paste0("AND ", select_logic))

    # Prepare query string to create temp table
    sql <- paste("CREATE TEMP TABLE", temp_tbl, "AS SELECT row_number() OVER () AS row,", sql_fields_units[1], "FROM (",
      "SELECT", sql_fields_units[2], "FROM", src_tbl,
      "WHERE emergency_admission = TRUE",
      additional_logic,
      "GROUP BY", sql_fields_units[3], ") AS st;")
  }

  return(sql)
}
