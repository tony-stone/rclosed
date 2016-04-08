# Creates new DB table to store only relevant HES AE records based on supplied LSOAs, aeattendcat, aedepttype
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


  # Get size of table
  nrows <- DBI::dbGetQuery(db_conn, "SELECT COUNT(*) FROM relevant_ae_attendances;")[1, 1]
  print(nrows)

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL
}

load("data/catchment area set final.Rda")

save_relevant_attendances(catchment_area_set_final[, lsoa])
