# Creates new DB table to store only relevant HES AE records based on supplied LSOAs, aeattendcat, aedepttype
save_relevant_attendances <- function(lsoas) {

  load(file = "data/DB settings read.rda")

  # Form the URL from the above set of params; ssl=true means we connect over SSL/TLS; Java, by default, checks server TLS/SSL certificate is valid
  db_url <- paste0("jdbc:postgresql://", db_config["host"], ":", db_config["port"], "/", db_config["db_name"], "?user=", db_config["user"], "&defaultAutoCommit")

  # Specify the driver, the Java driver (JAR) file, and that Postgres uses double quotes (") to escape identifiers (keywords)
  db_drvr <- RJDBC::JDBC("org.postgresql.Driver", "C:/Program Files/PostgreSQL/postgresql-9.4.1207.jar", "\"")

  # Set up our DB connection
  db_conn <- DBI::dbConnect(db_drvr, db_url, password = db_config["pass"])

  # Remove table if it already exists
  RJDBC::dbRemoveTable(db_conn, "relevant_ae_attendances")

  # Prepare query string to create table
  sql_query_make_table <- paste("CREATE TABLE relevant_ae_attendances AS",
    "SELECT * FROM public.hes_ae_0714_raw wHERE aeattendcat = '1' AND (aedepttype = '01' OR aedepttype = '99')",
    "AND LSOA01 IN (",
    paste0("'", lsoas, "'", collapse = ", "),
    ");", sep = " ")

  # Takes about 1.5hrs
  resource <- RJDBC::dbSendUpdate(db_conn, sql_query_make_table)

  # Get size of table - ~7M
  nrows <- DBI::dbGetQuery(db_conn, "SELECT COUNT(*) FROM relevant_ae_attendances;")[1, 1]

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  return(nrows)
}


# Creates new DB table to store only relevant HES APC records based on supplied LSOAs
save_relevant_admitted_care <- function(lsoas) {

  load(file = "data/DB settings read.rda")

  # Form the URL from the above set of params; ssl=true means we connect over SSL/TLS; Java, by default, checks server TLS/SSL certificate is valid
  db_url <- paste0("jdbc:postgresql://", db_config["host"], ":", db_config["port"], "/", db_config["db_name"], "?user=", db_config["user"], "&defaultAutoCommit")

  # Specify the driver, the Java driver (JAR) file, and that Postgres uses double quotes (") to escape identifiers (keywords)
  db_drvr <- RJDBC::JDBC("org.postgresql.Driver", "C:/Program Files/PostgreSQL/postgresql-9.4.1207.jar", "\"")

  # Set up our DB connection
  db_conn <- DBI::dbConnect(db_drvr, db_url, password = db_config["pass"])

  # Remove table if it already exists
  RJDBC::dbRemoveTable(db_conn, "relevant_apc_episodes")

  # Prepare query string to create table
  sql_query_make_table <- paste("CREATE TABLE relevant_apc_episodes AS",
    "SELECT * FROM public.hes_apc_0714_raw wHERE LSOA01 IN (",
    paste0("'", lsoas, "'", collapse = ", "),
    ");", sep = " ")

  # Takes about 3.5hrs
  resource <- RJDBC::dbSendUpdate(db_conn, sql_query_make_table)

  # Get size of table - ~10M
  nrows <- DBI::dbGetQuery(db_conn, "SELECT COUNT(*) FROM relevant_apc_episodes;")[1, 1]

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  return(nrows)
}


load("data/catchment area set final.Rda")
save_relevant_attendances(unique(catchment_area_set_final[, lsoa]))
