# Function to read in annual AE HES data and convert to annual attendances by procode/lsoa/month
library(data.table)

save_relevant_attendances <- function(lsoas) {

  load(file = "data/DB settings read.rda")

  # Form the URL from the above set of params; ssl=true means we connect over SSL/TLS; Java, by default, checks server TLS/SSL certificate is valid
  db_url <- paste0("jdbc:postgresql://", db_config["host"], ":", db_config["port"], "/", db_config["db_name"], "?user=", db_config["user"], "&defaultAutoCommit")

  # Specify the driver, the Java driver (JAR) file, and that Postgres uses double quotes (") to escape identifiers (keywords)
  db_drvr <- RJDBC::JDBC("org.postgresql.Driver", "C:/Program Files/PostgreSQL/postgresql-9.4.1207.jar", "\"")

  # Set up our DB connection
  db_conn <- DBI::dbConnect(db_drvr, db_url, password = db_config["pass"])

  # Prepare query string to create temp table
  sql_query_make_table <- paste("CREATE TABLE relevant_ae_attendances AS",
    "SELECT * FROM public.hes_ae_0714_raw wHERE aeattendcat = '1' AND (aedepttype = '01' OR aedepttype = '99')",
    "AND LSOA01 IN (",
    paste0("'", lsoas, "'", collapse = ", "),
    ");", sep = " ")

  # Takes about mins
  pc <- proc.time()
  resource <- RJDBC::dbSendUpdate(db_conn, sql_query_make_table)
  print(proc.time() - pc)


  # Get size of temp table
  nrows <- DBI::dbGetQuery(db_conn, "SELECT COUNT(*) FROM relevant_ae_attendances;")[1, 1]
  print(nrows)
#   # Set offset and limit var
#   offset <- 0L
#   limit <- 500000L
#
#   # Query table - takes ~12mins
#   sql_query_select <- paste0("SELECT procode3, lsoa01, aedepttype, yearmonth, attendances ",
#     "FROM admissions_by_trust_lsoa_month ",
#     "WHERE row > ", offset, " AND row <= ", offset + limit, ";")
#   attendances_by_trust_lsoa_month <- DBI::dbGetQuery(db_conn, sql_query_select)
#
#   # Need to call gc() to clear up Java heap space
#   gc()
#   # Increment offset
#   offset <- offset + limit
#
#   # Loop to fetch all rows of data (takes ~45s in entirety)
#   while (offset < nrows) {
#     sql_query_select <- paste0("SELECT procode3, lsoa01, aedepttype, yearmonth, attendances ",
#       "FROM admissions_by_trust_lsoa_month ",
#       "WHERE row > ", offset, " AND row <= ", offset + limit, ";")
#     attendances_by_trust_lsoa_month <- rbind(attendances_by_trust_lsoa_month, DBI::dbGetQuery(db_conn, sql_query_select))
#     gc()
#     offset <- offset + limit
#   }
#
  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

#   # Convert to data.table
#   attendances_by_trust_lsoa_month <- data.table::data.table(attendances_by_trust_lsoa_month)
#
#   # Convert dates to date type
#   attendances_by_trust_lsoa_month[, yearmonth := as.Date(lubridate::fast_strptime(paste0(yearmonth, "-01"), format = "%Y-%m-%d"))]
#
#   # Standardise field names
#   setnames(attendances_by_trust_lsoa_month, "lsoa01", "lsoa")
#
#   # save data
#   save(attendances_by_trust_lsoa_month, file = "data/attendances by trust lsoa month.Rda", compress = "xz")
}

load("data/catchment area set final.Rda")

save_relevant_attendances(catchment_area_set_final[, lsoa])
