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
save_relevant_admitted_care_episodes <- function() {

  db_conn <- connect2DB()

  tbl_name <- "relevant_apc_episodes"

  # Remove table if it already exists
  deleteDBTable(db_conn, tbl_name)

  # Prepare query string to create table
  sql_query_make_table <- paste("CREATE TABLE", tbl_name, "AS",
    "SELECT DISTINCT ON (encrypted_hesid, procode3, diag_01, diag_02, cause, admidate, disdate, epistart, epiend, epiorder, admimeth, admisorc, dismeth, tretspef)",
    "encrypted_hesid, tretspef, procode3, procode, epikey, lsoa01,",
    "CASE WHEN raw.startage = '' THEN NULL WHEN raw.startage IN (", paste0("'", 7001:7007, "'", collapse = ", "), ") THEN '0' ELSE raw.startage::integer END AS startage,",
    "CASE WHEN raw.endage = '' THEN NULL WHEN raw.endage IN (", paste0("'", 7001:7007, "'", collapse = ", "), ") THEN '0' ELSE raw.endage::integer END AS endage,",
    "CASE WHEN raw.sex <> '1' AND raw.sex <> '2' THEN NULL ELSE raw.sex END AS sex,",
    "CASE WHEN raw.admidate IS NULL OR raw.admidate IN ('', '1800-01-01', '1801-01-01') THEN NULL ELSE to_date(raw.admidate, 'YYYY-MM-DD') END AS admidate,",
    "CASE WHEN raw.epistart IN ('', '1800-01-01', '1801-01-01') THEN NULL ELSE to_date(raw.epistart, 'YYYY-MM-DD') END AS epistart,",
    "CASE WHEN raw.epiend IN ('', '1800-01-01', '1801-01-01') THEN NULL ELSE to_date(raw.epiend, 'YYYY-MM-DD') END AS epiend,",
    "CASE WHEN raw.disdate IN ('', '1800-01-01', '1801-01-01') THEN NULL ELSE to_date(raw.disdate, 'YYYY-MM-DD') END AS disdate,",
    "CASE WHEN raw.epiorder = '98' OR raw.epiorder = '99' THEN NULL ELSE raw.epiorder::integer END AS epiorder,",
    "CASE WHEN raw.admisorc = '98' OR raw.admisorc = '99' THEN NULL ELSE raw.admisorc END AS admisorc,",
    "CASE WHEN raw.disdest = '' OR raw.disdest = '98' OR raw.disdest = '99' THEN NULL ELSE raw.disdest END AS disdest,",
    "CASE WHEN raw.admimeth = '98' OR raw.admimeth = '99' THEN NULL ELSE raw.admimeth END AS admimeth,",
    "CASE WHEN raw.dismeth = ' ' OR raw.dismeth = '8' OR raw.dismeth = '9' THEN NULL ELSE raw.dismeth END AS dismeth,",
    "CASE WHEN raw.diag_01 IS NULL OR raw.diag_01 = '' THEN 'R69X6' ELSE raw.diag_01 END AS diag_01,",
    "CASE WHEN raw.diag_02 = '' THEN NULL ELSE raw.diag_02 END AS diag_02,",
    "CASE WHEN raw.cause = '' THEN NULL ELSE raw.cause END AS cause,",
    "CASE WHEN raw.admidate IS NULL OR raw.admidate = '' OR raw.admidate = '1800-01-01' THEN 1 WHEN raw.admidate = '1801-01-01' THEN 2 ELSE 0 END AS admidate_validity,",
    "CASE WHEN raw.epistart IS NULL OR raw.epistart = '' OR raw.epistart = '1800-01-01' THEN 1 WHEN raw.epistart = '1801-01-01' THEN 2 ELSE 0 END AS epistart_validity,",
    "CASE WHEN raw.epiend IS NULL OR raw.epiend = '' OR raw.epiend = '1800-01-01' THEN 1 WHEN raw.epiend = '1801-01-01' THEN 2 ELSE 0 END AS epiend_validity,",
    "CASE WHEN raw.disdate IS NULL OR raw.disdate = '' OR raw.disdate = '1800-01-01' THEN 1 WHEN raw.disdate = '1801-01-01' THEN 2 ELSE 0 END AS disdate_validity,",
    "CASE WHEN raw.disdest IN ('51', '52', '53') AND (raw.admisorc IS NULL OR raw.admisorc NOT IN ('51', '52', '53')) AND (raw.admimeth IS NULL OR raw.admimeth <> '81') THEN 1",
    "WHEN raw.disdest IN ('51', '52', '53') AND (raw.admisorc IN ('51', '52', '53') OR raw.admimeth = '81') THEN 2",
    "WHEN (raw.admisorc IN ('51', '52', '53') OR raw.admimeth = '81') AND (raw.disdest IS NULL OR raw.disdest NOT IN ('51', '52', '53')) THEN 3",
    "ELSE 0 END AS transit",
    "FROM public.hes_apc_0714_raw AS raw;")

  # Takes about 1.5hrs
  resource <- RJDBC::dbSendUpdate(db_conn, sql_query_make_table)

  # Get size of table - ~10M
  stats <- DBI::dbGetQuery(db_conn, paste("SELECT COUNT(*) AS total_records,",
    "SUM(CASE WHEN admidate IS NULL THEN 1 ELSE 0 END) AS invalid_admidate,",
    "SUM(CASE WHEN epistart IS NULL THEN 1 ELSE 0 END) AS invalid_epistart,",
    "SUM(CASE WHEN epiend IS NULL THEN 1 ELSE 0 END) AS invalid_epiend,",
    "SUM(CASE WHEN admidate IS NULL OR epistart IS NULL OR epiend IS NULL THEN 1 ELSE 0 END) AS invalid_record,",
    "SUM(CASE WHEN disdate IS NULL THEN 1 ELSE 0 END) AS invalid_disdate,",
    "SUM(CASE WHEN dismeth IS NULL THEN 1 ELSE 0 END) AS invalid_dismeth",
    "FROM", tbl_name, ";"))

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  return(stats)
}


# Creates new DB table to store only relevant HES APC spells
save_relevant_admitted_care_spells <- function() {

  db_conn <- connect2DB()

  tbl_name <- "relevant_apc_spells_linktable"

  deleteDBTable(db_conn, tbl_name)

  # Prepare query string to rank episodes
  sql_query_make_table <- paste("CREATE TEMP TABLE temp_episode_spelling AS",
    "SELECT row_number() OVER (PARTITION BY encrypted_hesid ORDER BY epistart ASC, epiend ASC, epiorder ASC, transit ASC, epikey ASC) AS spell_number, encrypted_hesid, procode3, dismeth, admidate, epistart, epiend, epiorder, epikey",
    "FROM (SELECT encrypted_hesid, procode3, epikey, admidate, epistart, epiend, dismeth, transit, epiorder FROM relevant_apc_episodes",
    "WHERE admidate IS NOT NULL AND epistart IS NOT NULL AND epiend IS NOT NULL) AS t1;", sep = " ")

  # Takes about 2mins
  resource <- RJDBC::dbSendUpdate(db_conn, sql_query_make_table)

  # Prepare query string to find earliest episode for each subsequent episode (if any) in a spell
  sql_create_intersect_query <- paste("CREATE TEMP TABLE temp_spell_number_updates AS",
    "SELECT DISTINCT ON (subsequent.epikey) subsequent.epikey, initial.epikey AS prev_epikey",
    "FROM temp_episode_spelling AS subsequent INNER JOIN temp_episode_spelling AS initial ON (",
    "subsequent.encrypted_hesid = initial.encrypted_hesid AND",
    "subsequent.procode3 = initial.procode3 AND",
    "subsequent.spell_number > initial.spell_number AND",
    "(",
    "subsequent.admidate = initial.admidate OR",
    "(",
    "subsequent.admidate <> initial.admidate AND ",
    "((initial.dismeth IS NULL AND subsequent.epistart = initial.epiend) OR subsequent.epistart = initial.epistart)",
    ")",
    ")) ORDER BY subsequent.epikey, initial.spell_number ASC NULLS LAST;")

  # Takes about 4mins
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_intersect_query)

  sql_num_records_to_update <- "SELECT COUNT(*) FROM temp_spell_number_updates AS subsequent INNER JOIN temp_spell_number_updates AS initial ON (subsequent.prev_epikey = initial.epikey);"

  sql_update_episodes_in_same_spell_query <- paste("UPDATE temp_spell_number_updates AS subsequent SET prev_epikey = initial.prev_epikey",
    "FROM temp_spell_number_updates AS initial WHERE subsequent.prev_epikey = initial.epikey;", sep = " ")

  records_to_update <- DBI::dbGetQuery(db_conn, sql_num_records_to_update)[1, 1]

  while(records_to_update > 0) {
    ## Update table
    resource <- RJDBC::dbSendUpdate(db_conn, sql_update_episodes_in_same_spell_query)
    records_to_update <- DBI::dbGetQuery(db_conn, sql_num_records_to_update)[1, 1]
  }

  sql_update_spell_combos_query <- paste("UPDATE temp_episode_spelling SET spell_number = episodes_to_update.spell_number FROM (",
    "SELECT temp_spell_number_updates.epikey, spell_numbers.spell_number FROM temp_spell_number_updates INNER JOIN temp_episode_spelling AS spell_numbers ON (temp_spell_number_updates.prev_epikey = spell_numbers.epikey)",
    ") AS episodes_to_update WHERE temp_episode_spelling.epikey = episodes_to_update.epikey;")

  # Take ~ 2mins
  resource <- RJDBC::dbSendUpdate(db_conn, sql_update_spell_combos_query)

  # Creat spells episode-level table
  sql_create_linktable_query <- paste("CREATE TABLE", tbl_name, "AS",
    "SELECT encrypted_hesid, dense_rank() OVER (PARTITION BY encrypted_hesid ORDER BY spell_number ASC) AS spell, row_number() OVER (PARTITION BY encrypted_hesid, spell_number ORDER BY epistart ASC NULLS LAST, epiend ASC NULLS LAST, dismeth DESC NULLS FIRST, epiorder ASC, epikey ASC) AS episode, epikey",
    "FROM temp_episode_spelling;")

  # Takes about 1.5mins
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_linktable_query)

  # Get size of table - ~10M
  nrows <- DBI::dbGetQuery(db_conn, paste0("SELECT COUNT(*) FROM ", tbl_name, ";"))[1, 1]

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  return(nrows)
}



save_relevant_admitted_care_cips <- function() {

  db_conn <- connect2DB()

  tbl_name <- "relevant_apc_cips_episode_data"

  deleteDBTable(db_conn, tbl_name)

  # Prepare query string to create table of distinct spells, the start date of the spell, the end date of the spell and also the spells earliest "transfer in" date (if any) and latest "transfer out" date (if any).
  sql_query_make_table <- paste("CREATE TEMP TABLE temp_spell_cipsing AS",
    "SELECT row_number() OVER (PARTITION BY encrypted_hesid ORDER BY spellstart ASC, spell ASC) AS cips, foo.* FROM (",
    "SELECT spells.*, spell_earliest_trans.transferedin, spell_latest_trans.transferedout FROM ",
    "(SELECT spell_id.encrypted_hesid, spell_id.spell, MIN(episodes.epistart) AS spellstart, MAX(episodes.epiend) AS spellend FROM relevant_apc_spells_linktable AS spell_id INNER JOIN relevant_apc_episodes AS episodes ON spell_id.epikey = episodes.epikey GROUP BY spell_id.encrypted_hesid, spell_id.spell) AS spells",
    "LEFT JOIN",
    "(SELECT spell_id.encrypted_hesid, spell_id.spell, MIN(episodes.epistart) AS transferedin FROM relevant_apc_spells_linktable AS spell_id INNER JOIN relevant_apc_episodes AS episodes ON spell_id.epikey = episodes.epikey WHERE episodes.admimeth = '81' OR episodes.admisorc IN ('51', '52', '53') GROUP BY spell_id.encrypted_hesid, spell_id.spell) AS spell_earliest_trans",
    "ON spells.encrypted_hesid = spell_earliest_trans.encrypted_hesid AND spells.spell = spell_earliest_trans.spell",
    "LEFT JOIN",
    "(SELECT spell_id.encrypted_hesid, spell_id.spell, MAX(episodes.epiend) AS transferedout FROM relevant_apc_spells_linktable AS spell_id INNER JOIN relevant_apc_episodes AS episodes ON spell_id.epikey = episodes.epikey WHERE episodes.disdest IN ('51', '52', '53') GROUP BY spell_id.encrypted_hesid, spell_id.spell) AS spell_latest_trans",
    "ON spells.encrypted_hesid = spell_latest_trans.encrypted_hesid AND spells.spell = spell_latest_trans.spell) AS foo;")

  # Takes about 5mins
  resource <- RJDBC::dbSendUpdate(db_conn, sql_query_make_table)

  # Add a unique ID and date fields - takes ~30s
  resource <- RJDBC::dbSendUpdate(db_conn, "ALTER TABLE temp_spell_cipsing ADD COLUMN uid SERIAL;")

  # Prepare query string to get set of spells which are part of a non-single-spell CIPS
  sql_create_intersect_query <- paste("CREATE TEMP TABLE temp_cips_number_updates AS",
    "SELECT DISTINCT ON (subsequent.uid) subsequent.uid, initial.uid AS prev_uid",
    "FROM temp_spell_cipsing AS subsequent INNER JOIN temp_spell_cipsing AS initial ON (",
    "subsequent.encrypted_hesid = initial.encrypted_hesid AND",
    "subsequent.cips > initial.cips AND",
    "(subsequent.spellstart <= initial.transferedout + 2 OR subsequent.transferedin <= initial.spellend + 2)",
    ") ORDER BY subsequent.uid ASC, initial.cips ASC;")

  # Takes about 2mins
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_intersect_query)

  sql_num_records_to_update <- "SELECT COUNT(*) FROM temp_cips_number_updates AS initial INNER JOIN temp_cips_number_updates AS subsequent ON (initial.uid = subsequent.prev_uid);"

  sql_update_episodes_in_same_cips_query <- paste("UPDATE temp_cips_number_updates AS subsequent SET prev_uid = initial.prev_uid",
    "FROM temp_cips_number_updates AS initial WHERE subsequent.prev_uid = initial.uid;", sep = " ")

  records_to_update <- DBI::dbGetQuery(db_conn, sql_num_records_to_update)[1, 1]

  while(records_to_update > 0) {
    ## Update table
    resource <- RJDBC::dbSendUpdate(db_conn, sql_update_episodes_in_same_cips_query)
    records_to_update <- DBI::dbGetQuery(db_conn, sql_num_records_to_update)[1, 1]
  }

  sql_update_cips_combos_query <- paste("UPDATE temp_spell_cipsing SET cips = spells_to_update.cips FROM (",
    "SELECT temp_cips_number_updates.uid, spell_cips.cips FROM temp_cips_number_updates INNER JOIN temp_spell_cipsing AS spell_cips ON (temp_cips_number_updates.prev_uid = spell_cips.uid)",
    ") AS spells_to_update WHERE temp_spell_cipsing.uid = spells_to_update.uid;")

  # Takes ~ 30s
  resource <- RJDBC::dbSendUpdate(db_conn, sql_update_cips_combos_query)

  # Prepare linktable
  sql_create_linktable_query <- paste("CREATE TEMP TABLE temp_relevant_apc_cips_linktable AS",
    "SELECT episodes.encrypted_hesid,",
    "dense_rank() OVER (PARTITION BY episodes.encrypted_hesid ORDER BY cipss.cips ASC) AS cips,",
    "dense_rank() OVER (PARTITION BY episodes.encrypted_hesid, cipss.cips ORDER BY episodes.spell ASC) AS cips_spell,",
    "row_number() OVER (PARTITION BY episodes.encrypted_hesid, cipss.cips ORDER BY episodes.spell ASC, episodes.episode ASC) AS cips_episode,",
    "episodes.spell, episodes.episode, epikey",
    "FROM relevant_apc_spells_linktable AS episodes LEFT JOIN temp_spell_cipsing AS cipss ON",
    "(episodes.encrypted_hesid = cipss.encrypted_hesid AND episodes.spell = cipss.spell);")

  # Takes about 2mins
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_linktable_query)

  # Create data data
  sql_create_data_table_query <- paste("CREATE TABLE", tbl_name, "AS",
    "SELECT relevant_apc_episodes.*, cipss.cips, cipss.cips_spell, cipss.cips_episode , cipss.spell, cipss.episode",
    "FROM relevant_apc_episodes INNER JOIN temp_relevant_apc_cips_linktable AS cipss ON",
    "(relevant_apc_episodes.epikey = cipss.epikey);")

  # Takes about 2mins
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_data_table_query)

  # Get size of table - ~10M
  nrows <- DBI::dbGetQuery(db_conn, paste0("SELECT COUNT(*) FROM ", tbl_name, ";"))[1, 1]

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  return(nrows)
}



save_relevant_cips_data <- function(lsoas) {

  db_conn <- connect2DB()

  tbl_name <- "relevant_apc_cips_data"

  deleteDBTable(db_conn, tbl_name)

  # Prepare query string to create table of summary data for each cips
  sql_query_make_table <- paste("CREATE TABLE", tbl_name, "AS",
    "SELECT cipss.*,",
    "CASE WHEN cipss.male_episodes > cipss.female_episodes THEN 1 WHEN cipss.male_episodes < cipss.female_episodes THEN 2 ELSE NULL END AS sex,",
    "discharged.last_disdate, died.date_of_death_first, died.date_of_death_last,",
    "admiepi.diag_01, admiepi.diag_02, admiepi.cause, admiepi.startage, admiepi.admimeth, admiepi.lsoa01, admiepi.procode, lastepi.endage,",
    "CASE WHEN discharged.last_disdate >= cipss.cips_end THEN TRUE ELSE FALSE END AS cips_finished,",
    "CASE WHEN died.date_of_death_first IS NOT NULL THEN TRUE ELSE FALSE END AS died,",
    "(cipss.cips_end - cipss.cips_start) AS nights_admitted,",
    "CASE WHEN admiepi.admimeth = '21' THEN TRUE ELSE FALSE END AS emergency_admission",
    "FROM",
    "(",
    "SELECT encrypted_hesid, cips, LEAST(MIN(admidate), MIN(epistart)) AS cips_start, MAX(epiend) AS cips_end,",
    "MAX(CASE WHEN tretspef = '192' THEN 1 ELSE 0 END) AS any_critical_care,",
    "SUM(CASE WHEN sex = '1' THEN 1 ELSE 0 END) AS male_episodes, SUM(CASE WHEN sex IS NOT DISTINCT FROM '2' THEN 1 ELSE 0 END) AS female_episodes,",
    "COUNT(*) AS total_episodes,",
    "MIN(startage) AS cips_youngestage, MAX(endage) AS cips_oldestage",
    "FROM relevant_apc_cips_episode_data GROUP BY encrypted_hesid, cips",
    ") AS cipss",
    "LEFT JOIN",
    "(SELECT encrypted_hesid, cips, MAX(disdate) AS last_disdate FROM relevant_apc_cips_episode_data WHERE disdate = epiend AND disdest NOT IN ('51', '52', '53') GROUP BY encrypted_hesid, cips) AS discharged",
    "ON (cipss.encrypted_hesid = discharged.encrypted_hesid AND cipss.cips = discharged.cips)",
    "LEFT JOIN",
    "(SELECT encrypted_hesid, cips, MIN(disdate) AS date_of_death_first, MAX(disdate) AS date_of_death_last FROM relevant_apc_cips_episode_data WHERE dismeth = '4' OR disdest = '79' GROUP BY encrypted_hesid, cips) AS died",
    "ON (cipss.encrypted_hesid = died.encrypted_hesid AND cipss.cips = died.cips)",
    "LEFT JOIN",
    "(SELECT encrypted_hesid, cips, diag_01, diag_02, cause, startage, admimeth, lsoa01, procode, cips_episode FROM relevant_apc_cips_episode_data) AS admiepi",
    "ON (cipss.encrypted_hesid = admiepi.encrypted_hesid AND cipss.cips = admiepi.cips AND admiepi.cips_episode = 1)",
    "LEFT JOIN",
    "(SELECT epis.encrypted_hesid, epis.cips, epis.endage FROM relevant_apc_cips_episode_data AS epis INNER JOIN",
    "(SELECT encrypted_hesid, cips, MAX(cips_episode) AS max_cips_episode FROM relevant_apc_cips_episode_data GROUP BY encrypted_hesid, cips) AS maxepi",
    "ON (epis.encrypted_hesid = maxepi.encrypted_hesid AND epis.cips = maxepi.cips AND epis.cips_episode = maxepi.max_cips_episode)) AS lastepi",
    "ON (cipss.encrypted_hesid = lastepi.encrypted_hesid AND cipss.cips = lastepi.cips)",
    "WHERE cipss.cips_start > to_date('2007-03-31', 'YYYY-MM-DD') AND admiepi.lsoa01 IN (", paste0("'", lsoas, "'", collapse = ", "), ");")

  # Takes about 6mins
  resource <- RJDBC::dbSendUpdate(db_conn, sql_query_make_table)

  # Get size of table - ~9M
  nrows <- DBI::dbGetQuery(db_conn, paste0("SELECT COUNT(*) FROM ", tbl_name, ";"))[1, 1]

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  return(nrows)
}


# Creates new DB table to store only relevant HES APC records based on supplied LSOAs
prepare_relevant_admitted_care <- function(lsoas) {

  n_episodes <- save_relevant_admitted_care_episodes()

  n_spells <- save_relevant_admitted_care_spells()

  n_cips <- save_relevant_admitted_care_cips()

  cips_data_rows <- save_relevant_cips_data(lsoas)
}


# library(data.table)
# load("data/catchment area set final.Rda")
# lsoas <- unique(catchment_area_set_final$lsoa)
# rm(catchment_area_set_final)
# gc()
# prepare_relevant_admitted_care(lsoas)
#
# # bla1 <- DBI::dbGetQuery(db_conn, paste("SELECT COUNT(*) AS total_cips, SUM(CASE WHEN died = TRUE THEN 1 ELSE 0 END) AS deaths, SUM(CASE WHEN cips_finished = TRUE THEN 1 ELSE 0 END) AS finished FROM relevant_apc_cips_data WHERE emergency_admission = TRUE;"))
# #
# # bla2 <- DBI::dbGetQuery(db_conn, paste("SELECT SUM(CASE WHEN male_episodes > 0 THEN 1 ELSE 0 END) AS males, SUM(CASE WHEN female_episodes > 0 THEN 1 ELSE 0 END) AS females, SUM(CASE WHEN female_episodes = 0 AND male_episodes = 0 THEN 1 ELSE 0 END) AS unknowns FROM relevant_apc_cips_data WHERE emergency_admission = TRUE;"))
# #
# # bla3 <- DBI::dbGetQuery(db_conn, paste("SELECT admimeth, COUNT(*) FROM relevant_apc_cips_data GROUP BY admimeth ORDER BY admimeth;"))
# #
# # bla4 <- DBI::dbGetQuery(db_conn, paste("SELECT nights_admitted FROM relevant_apc_cips_data WHERE cips_finished = TRUE AND emergency_admission = TRUE AND nights_admitted < 213;"))
# #
# # bla5 <- DBI::dbGetQuery(db_conn, paste("SELECT DISTINCT lsoa01 FROM relevant_apc_cips_data WHERE cips_finished = TRUE AND emergency_admission = TRUE;"))
# #
# # bla6 <- DBI::dbGetQuery(db_conn, paste("SELECT encrypted_hesid FROM relevant_apc_cips_data WHERE cips_finished = TRUE AND emergency_admission = TRUE AND last_disdate <> date_of_death_last;"))
# #
# # zombies <- DBI::dbGetQuery(db_conn, paste("SELECT * FROM relevant_apc_episodes WHERE encrypted_hesid IN (", paste0("'", bla6$encrypted_hesid, "'", collapse = ", "), ");"))
# #
# # Strange finishes
# stange_finishers <- DBI::dbGetQuery(db_conn, paste("SELECT dis.dis_diff, COUNT(*) FROM (SELECT (cips_end - last_disdate) AS dis_diff FROM relevant_apc_cips_data) AS dis GROUP BY dis.dis_diff ORDER BY dis.dis_diff ASC NULLS FIRST;"))
# sum(stange_finishers$count[!is.na(stange_finishers$dis_diff) & stange_finishers$dis_diff < 0])
#
# # Emergency and avoidable admissions
# measures1 <- DBI::dbGetQuery(db_conn, paste("SELECT to_char(date_trunc('month', cips_start), 'YYYY-MM-DD') AS yearmonth, lsoa01 AS lsoa, startage, diag_01, diag_02, cause, COUNT(*) AS values FROM relevant_apc_cips_data WHERE emergency_admission = TRUE GROUP BY yearmonth, lsoa, startage, diag_01, diag_02, cause;"))
#
# # condition severity
# measures2 <- DBI::dbGetQuery(db_conn, paste("SELECT to_char(date_trunc('month', cips_start), 'YYYY-MM-DD') AS yearmonth, lsoa01 AS lsoa, AVG(CAST(nights_admitted AS DOUBLE PRECISION)) AS mean_length_of_stay, COUNT(*) as num_admissions, SUM(any_critical_care) AS num_received_critical_care FROM relevant_apc_cips_data WHERE cips_finished = TRUE AND emergency_admission = TRUE AND nights_admitted < 213 GROUP BY yearmonth, lsoa;"))
#
# # mortality
# measures3 <- DBI::dbGetQuery(db_conn, paste("SELECT to_char(date_trunc('month', cips_start), 'YYYY-MM-DD') AS yearmonth, to_char(date_trunc('month', date_of_death_last), 'YYYY-MM-DD') AS yearmonth_death, lsoa01 AS lsoa, startage, endage, diag_01, diag_02, cause FROM relevant_apc_cips_data WHERE cips_finished = TRUE AND emergency_admission = TRUE AND nights_admitted < 213 AND died = TRUE AND date_of_death_last = last_disdate;"))
#
# # Case fatality
# measures4 <- DBI::dbGetQuery(db_conn, paste("SELECT to_char(date_trunc('month', cips_start), 'YYYY-MM-DD') AS yearmonth, lsoa01 AS lsoa, startage, diag_01, diag_02, cause, COUNT(*) AS cases, SUM(CASE WHEN died = TRUE AND date_of_death_last = last_disdate AND nights_admitted < 3 THEN 1 ELSE 0) AS fatalities FROM relevant_apc_cips_data WHERE emergency_admission = TRUE GROUP BY yearmonth, lsoa, startage, diag_01, diag_02, cause;"))
#
# # ggplot2::ggplot(bla4, ggplot2::aes(nights_admitted)) +
# #   ggplot2::ggtitle("Histogram of nights admitted for patients admitted from A&E") +
# #   ggplot2::geom_histogram(binwidth = 1) +
# #   ggplot2::scale_y_continuous(labels = scales::comma, limits = c(0, NA), expand = c(0.02, 0))


