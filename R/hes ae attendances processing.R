# Function to read in annual AE HES data and convert to annual attendances by procode/lsoa/month

save_HES_AE_attendances_data <- function() {
  # Set up our DB connection
  db_conn <- connect2DB()

  tbl_name <- "admissions_by_trust_lsoa_month"
  add_logic <- "aeattendcat = '1' AND (aedepttype = '01' OR aedepttype = '99')"
  add_fields <- ""

  # Prepare query string to create temp table
  sql_create_tbl <- getSqlUpdateQuery("apc", tbl_name, add_logic, add_fields)

  # Takes ~2mins
  pc <- proc.time()
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_tbl)
  proc.time() - pc

  # retrieve data
  attendances_by_trust_lsoa_month <- getDataFromTempTable(db_conn, tbl_name, "ae", add_fields)

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  # save data
  save(attendances_by_trust_lsoa_month, file = "data/attendances by trust lsoa month.Rda", compress = "xz")
}
