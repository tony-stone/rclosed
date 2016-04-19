# Creates new DB table to store only relevant HES AE records based on supplied LSOAs, aeattendcat, aedepttype
prepare_relevant_attendances <- function(lsoas) {
  db_conn <- connect2DB()

  # Remove table if it already exists
  if(RJDBC::dbExistsTable(db_conn, "relevant_ae_attendances") == TRUE) RJDBC::dbRemoveTable(db_conn, "relevant_ae_attendances")

  # Prepare query string to create table
  sql_query_make_table <- paste("CREATE TABLE relevant_ae_attendances AS",
    "SELECT * FROM public.hes_ae_0714_raw wHERE LSOA01 IN (", paste0("'", lsoas, "'", collapse = ", "), ");", sep = " ")

  # Takes about 1.5hrs
  resource <- RJDBC::dbSendUpdate(db_conn, sql_query_make_table)

  # Get size of table - ~8.9M
  nrows <- DBI::dbGetQuery(db_conn, "SELECT COUNT(*) FROM relevant_ae_attendances;")[1, 1]

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  return(nrows)
}


# Creates new DB table to store only relevant HES APC records based on supplied LSOAs
save_relevant_admitted_care <- function(lsoas) {

  db_conn <- connect2DB()

  # Remove table if it already exists
  if(RJDBC::dbExistsTable(db_conn, "relevant_apc_episodes") == TRUE) RJDBC::dbRemoveTable(db_conn, "relevant_apc_episodes")

  # Prepare query string to create table
  sql_query_make_table <- paste("CREATE TABLE relevant_apc_episodes AS",
    "SELECT DISTINCT ON (encrypted_hesid, diag_01, admidate, disdate, epistart, epiend, epiorder, mainspef, tretspef) * FROM public.hes_apc_0714_raw ",
    "wHERE LSOA01 IN (", paste0("'", lsoas, "'", collapse = ", "), ");", sep = " ")

  # Takes about 3.5hrs
  resource <- RJDBC::dbSendUpdate(db_conn, sql_query_make_table)

  # Get size of table - ~10M
  nrows <- DBI::dbGetQuery(db_conn, "SELECT COUNT(*) FROM relevant_apc_episodes;")[1, 1]

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  return(nrows)
}



# Creates new DB table to store only relevant HES APC records based on supplied LSOAs
prepare_relevant_admitted_care <- function(lsoas) {

  nrows <- save_relevant_admitted_care(lsoas)

#   db_conn <- connect2DB()
#
#   # Prepare query string to create table
#   sql_query_make_table <- paste("CREATE TABLE relevant_apc_episodes AS",
#     "SELECT * FROM public.hes_apc_0714_raw wHERE LSOA01 IN (",
#     paste0("'", lsoas, "'", collapse = ", "),
#     ");", sep = " ")
#
#   # Takes about 3.5hrs
#   resource <- RJDBC::dbSendUpdate(db_conn, sql_query_make_table)
#
#   # Get size of table - ~10M
#   nrows <- DBI::dbGetQuery(db_conn, "SELECT COUNT(*) FROM relevant_apc_episodes;")[1, 1]
#
#   # Disconnect from DB
#   DBI::dbDisconnect(db_conn)
#   db_conn <- NULL

  return(nrows)
}

# db_conn <- connect2DB()
# hes_apc_eg <- DBI::dbGetQuery(db_conn, "SELECT * FROM relevant_apc_episodes WHERE LSOA01 = 'E01019966';")

# library(data.table)
# load("data/catchment area set final.Rda")
# prepare_relevant_admitted_care(unique(catchment_area_set_final[, lsoa]))
# prepare_relevant_attendances(unique(catchment_area_set_final[, lsoa]))
