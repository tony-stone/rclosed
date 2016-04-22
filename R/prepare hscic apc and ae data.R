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


# Creates new DB table to store only relevant HES APC episodes based on supplied LSOAs
save_relevant_admitted_care_episodes <- function(lsoas) {

  db_conn <- connect2DB()

  # Remove table if it already exists
  if(RJDBC::dbExistsTable(db_conn, "relevant_apc_episodes") == TRUE) RJDBC::dbRemoveTable(db_conn, "relevant_apc_episodes")

  # Prepare query string to create table
  sql_query_make_table <- paste("CREATE TABLE relevant_apc_episodes AS",
    "SELECT DISTINCT ON (encrypted_hesid, diag_01, admidate, disdate, epistart, epiend, epiorder, mainspef, tretspef) * FROM public.hes_apc_0714_raw ",
    "wHERE LSOA01 IN (", paste0("'", lsoas, "'", collapse = ", "), ");", sep = " ")

  # Takes about 3.5hrs
  resource <- RJDBC::dbSendUpdate(db_conn, sql_query_make_table)

  # Small bit of data cleaning
  resource <- RJDBC::dbSendUpdate(db_conn, "UPDATE relevant_apc_episodes SET dismeth = NULL WHERE dismeth = ' ';")

  # Get size of table - ~10M
  nrows <- DBI::dbGetQuery(db_conn, "SELECT COUNT(*) FROM relevant_apc_episodes;")[1, 1]

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  return(nrows)
}


# Creates new DB table to store only relevant HES APC spells
# - keep:
# -- (diag_nn, cause_nn, admidate, startage) from admission episode;
# -- (disdate, dismeth, endage) of discharge episode; any_critical_care is derived from tretspef from all episodes
save_relevant_admitted_care_spells <- function() {

  db_conn <- connect2DB()

  tbl_name <- "relevant_apc_spells_linktable"

  deleteDBTable(db_conn, tbl_name)

  # Prepare query string to create table of distinct combinations of (encrypted_hesid, procode3, admidate, dismeth, epistart, epiend)
  sql_query_make_table <- paste("CREATE TEMP TABLE distinct_spell_combos AS",
    "SELECT row_number() OVER (PARTITION BY encrypted_hesid ORDER BY admidate ASC, epistart ASC, epiend ASC, dismeth DESC) AS spell_number, encrypted_hesid, procode3, admidate, dismeth, epistart, epiend",
    "FROM (SELECT DISTINCT encrypted_hesid, procode3, admidate, dismeth, epistart, epiend FROM relevant_apc_episodes) AS t1;", sep = " ")

  # Takes about 2.5mins
  resource <- RJDBC::dbSendUpdate(db_conn, sql_query_make_table)

  # Add a unique ID column - takes ~30s
  resource <- RJDBC::dbSendUpdate(db_conn, "ALTER TABLE distinct_spell_combos ADD COLUMN uid SERIAL;")

  # Prepare query string to create new intersect table of distinct combinations of (encrypted_hesid, procode3, admidate, dismeth, epistart, epiend)
  sql_create_intersect_query <- paste("CREATE TEMP TABLE episodes_in_same_spell AS",
    "SELECT DISTINCT ON (t1.uid) t1.uid, t2.uid AS prev_uid",
    "FROM distinct_spell_combos AS t1 INNER JOIN distinct_spell_combos AS t2 ON (",
    "t1.spell_number > t2.spell_number AND",
    "t1.encrypted_hesid = t2.encrypted_hesid AND",
    "t1.procode3 = t2.procode3 AND",
    "(",
    "t1.admidate = t2.admidate OR",
    "(t1.admidate != t2.admidate AND",
    "(((t2.dismeth IS NULL OR t2.dismeth = ' ' OR t2.dismeth = '8' OR t2.dismeth = '9') AND t2.epiend = t1.epistart) OR t1.epistart = t2.epistart))",
    ")) ORDER BY t1.uid, t2.spell_number;", sep = " ")

  # Takes about 7mins
  pc <- proc.time()
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_intersect_query)
  proc.time() - pc

  sql_num_records_to_update <- "SELECT COUNT(*) FROM episodes_in_same_spell AS t1 INNER JOIN episodes_in_same_spell AS t2 ON (t1.uid = t2.prev_uid);"

  sql_update_episodes_in_same_spell_query <- paste("UPDATE episodes_in_same_spell AS t1 SET prev_uid = t2.prev_uid",
    "FROM episodes_in_same_spell AS t2 WHERE t1.prev_uid = t2.uid;", sep = " ")

  records_to_update <- DBI::dbGetQuery(db_conn, sql_num_records_to_update)[1, 1]

  while(records_to_update > 0) {

    ## Update table
    pc <- proc.time()
    resource <- RJDBC::dbSendUpdate(db_conn, sql_update_episodes_in_same_spell_query)
    proc.time() - pc

    records_to_update <- DBI::dbGetQuery(db_conn, sql_num_records_to_update)[1, 1]
  }

  sql_update_spell_combos_query <- paste("UPDATE distinct_spell_combos AS t1 SET spell_number = t2.spell_number FROM (",
    "SELECT DISTINCT t3.uid, t4.spell_number FROM episodes_in_same_spell AS t3 INNER JOIN distinct_spell_combos AS t4 ON (t3.prev_uid = t4.uid)",
    ") AS t2 WHERE t1.uid = t2.uid;", sep = " ")

  # Take ~ 1.5mins
  pc <- proc.time()
  resource <- RJDBC::dbSendUpdate(db_conn, sql_update_spell_combos_query)
  proc.time() - pc


  # Prepare linktable
  sql_create_linktable_query <- paste("CREATE TABLE", tbl_name, "AS",
    "SELECT encrypted_hesid, dense_rank() OVER (PARTITION BY encrypted_hesid ORDER BY spell_number ASC) AS spell, row_number() OVER (PARTITION BY encrypted_hesid, spell_number ORDER BY epistart ASC, epiend ASC, dismeth DESC) AS episode, epikey",
    "FROM (",
    "SELECT t1.encrypted_hesid, t1.spell_number, t1.procode3, t1.admidate, t1.dismeth, t1.epistart, t1.epiend, t2.epikey",
    "FROM distinct_spell_combos AS t1 LEFT JOIN relevant_apc_episodes AS t2 ON (",
    "t1.encrypted_hesid = t2.encrypted_hesid AND",
    "t1.procode3 = t2.procode3 AND",
    "t1.admidate = t2.admidate AND",
    "t1.dismeth = t2.dismeth AND",
    "t1.epiend = t2.epiend AND",
    "t1.epistart = t2.epistart)) AS tb;", sep = " ")

  # Takes about 4.5mins
  pc <- proc.time()
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_linktable_query)
  proc.time() - pc

  # Get size of table - ~10M
  nrows <- DBI::dbGetQuery(db_conn, paste0("SELECT COUNT(*) FROM ", "relevant_apc_episodes", ";"))[1, 1]

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  return(nrows)
}



save_relevant_admitted_care_cips <- function() {

  db_conn <- connect2DB()

  tbl_name <- "relevant_apc_cips_linktable"

  deleteDBTable(db_conn, tbl_name)
  #deleteDBTable(db_conn, "distinct_cips_combos")

  # Prepare query string to create table of distinct combinations of (encrypted_hesid, spell, epistart, epiend, disdest, admisorc, admimeth)
  sql_query_make_table <- paste("CREATE TEMP TABLE distinct_cips_combos AS",
    "SELECT row_number() OVER (PARTITION BY encrypted_hesid ORDER BY epistart, epiend) AS cips_number, encrypted_hesid, spell, epistart, epiend, admisorc, disdest, admimeth",
    "FROM (SELECT DISTINCT epis.encrypted_hesid, epis.epistart, epis.epiend, epis.admisorc, epis.disdest, epis.admimeth, spells.spell",
    "FROM relevant_apc_episodes AS epis INNER JOIN relevant_apc_spells_linktable AS spells ON (epis.epikey = spells.epikey)",
    ") AS t1;", sep = " ")

  # Takes about 4mins
  resource <- RJDBC::dbSendUpdate(db_conn, sql_query_make_table)

  # Add a unique ID and date fields - takes ~30s
  resource <- RJDBC::dbSendUpdate(db_conn, "ALTER TABLE distinct_cips_combos ADD COLUMN uid SERIAL, ADD COLUMN epistart_date DATE, ADD COLUMN epiend_date DATE;")

  # Set dates
  resource <- RJDBC::dbSendUpdate(db_conn, "UPDATE distinct_cips_combos SET (epistart_date, epiend_date) = (to_date(epistart, 'YYYY-MM-DD'), to_date(epiend, 'YYYY-MM-DD'));")

  # Prepare query string to create new intersect table of distinct combinations of (encrypted_hesid, spell, epistart, epiend, transfered_to, transfered_from, transfered)
  sql_create_intersect_query <- paste("CREATE TEMP TABLE episodes_in_same_cip AS",
    "SELECT DISTINCT ON (t1.uid) t1.uid, t2.uid AS prev_uid",
    "FROM distinct_cips_combos AS t1 INNER JOIN distinct_cips_combos AS t2 ON (",
    "t1.cips_number > t2.cips_number AND",
    "t1.encrypted_hesid = t2.encrypted_hesid AND (",
    "t1.spell = t2.spell OR (",
    "t1.epistart_date >= t2.epiend_date AND t1.epistart_date <= t2.epiend_date + 2 AND",
    "(t2.disdest IN ('51', '52', '53') OR t1.admisorc IN ('51', '52', '53') OR t1.admimeth = '81')",
    "))) ORDER BY t1.uid, t2.cips_number;", sep = " ")

  # Takes about 5mins
  pc <- proc.time()
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_intersect_query)
  proc.time() - pc

  sql_num_records_to_update <- "SELECT COUNT(*) FROM episodes_in_same_cip AS t1 INNER JOIN episodes_in_same_cip AS t2 ON (t1.uid = t2.prev_uid);"

  sql_update_episodes_in_same_spell_query <- paste("UPDATE episodes_in_same_cip AS t1 SET prev_uid = t2.prev_uid",
    "FROM episodes_in_same_cip AS t2 WHERE t1.prev_uid = t2.uid;", sep = " ")

  records_to_update <- DBI::dbGetQuery(db_conn, sql_num_records_to_update)[1, 1]

  while(records_to_update > 0) {

    ## Update table
    pc <- proc.time()
    resource <- RJDBC::dbSendUpdate(db_conn, sql_update_episodes_in_same_spell_query)
    proc.time() - pc

    records_to_update <- DBI::dbGetQuery(db_conn, sql_num_records_to_update)[1, 1]
  }

  sql_update_cips_combos_query <- paste("UPDATE distinct_cips_combos AS t1 SET cips_number = t2.cips_number FROM (",
    "SELECT DISTINCT t3.uid, t4.cips_number FROM episodes_in_same_cip AS t3 INNER JOIN distinct_cips_combos AS t4 ON (t3.prev_uid = t4.uid)",
    ") AS t2 WHERE t1.uid = t2.uid;", sep = " ")

  # Takes ~ 1.5mins
  pc <- proc.time()
  resource <- RJDBC::dbSendUpdate(db_conn, sql_update_cips_combos_query)
  proc.time() - pc


  # Prepare linktable
  sql_create_linktable_query <- paste("CREATE TABLE", tbl_name, "AS",
    "SELECT encrypted_hesid, dense_rank() OVER (PARTITION BY encrypted_hesid ORDER BY cips_number ASC) AS cips, dense_rank() OVER (PARTITION BY encrypted_hesid, cips_number ORDER BY spell ASC) AS spell, row_number() OVER (PARTITION BY encrypted_hesid, cips_number ORDER BY epistart ASC, epiend ASC) AS episode, epikey",
    "FROM (",
    "SELECT t1.encrypted_hesid, t1.cips_number, t1.spell, t1.epistart, t1.epiend, t2.epikey",
    "FROM distinct_cips_combos AS t1 LEFT JOIN relevant_apc_episodes AS t2 ON (",
    "t1.encrypted_hesid = t2.encrypted_hesid AND",
    "t1.disdest = t2.disdest AND",
    "t1.admisorc = t2.admisorc AND",
    "t1.admimeth = t2.admimeth AND",
    "t1.epiend = t2.epiend AND",
    "t1.epistart = t2.epistart)) AS tb;", sep = " ")

  # Takes about 5mins
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_linktable_query)

  # Get size of table - ~10M
  nrows <- DBI::dbGetQuery(db_conn, paste0("SELECT COUNT(*) FROM ", tbl_name, ";"))[1, 1]

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  return(nrows)
}




# Creates new DB table to store only relevant HES APC records based on supplied LSOAs
prepare_relevant_admitted_care <- function(lsoas) {

  n_episodes <- save_relevant_admitted_care_episodes(lsoas)

  n_spells <- save_relevant_admitted_care_spells()

  n_cips <- save_relevant_admitted_care_cips()

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
