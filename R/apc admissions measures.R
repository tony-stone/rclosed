save_emergency_admissions_measure <- function() {

  db_conn <- connect2DB()

  tbl_name <- "emergency_admissions_by_diagnosis_site_lsoa_month"
  add_logic <- "epiorder = '1' AND (admimeth = '21' OR admimeth = '2A')"
  add_fields <- "admimeth, diag_01, diag_02, cause, startage"

  # Prepare query string to create temp table
  sql_create_tbl <- getSqlUpdateQuery("apc", tbl_name, add_logic, add_fields)

  # Takes ~2mins
  pc <- proc.time()
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_tbl)
  proc.time() - pc

  # retrieve data
  emergency_admissions_by_diagnosis_site_lsoa_month <- getDataFromTempTable(db_conn, tbl_name, "apc", add_fields)

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  ## Deal with error codes in the diag_01 field
  # cause code in primary diagnosis field
  emergency_admissions_by_diagnosis_site_lsoa_month[diag_01 == "R69X3", diag_01 := diag_02]

  ## categorise into avoidable conditions
  # default to 'other'
  emergency_admissions_by_diagnosis_site_lsoa_month[, sub_measure := "other"]

  # Create 1 character, 3 character and 4 character codes diagnosis codes and 3 character cause code
  emergency_admissions_by_diagnosis_site_lsoa_month[, diag_4char := toupper(substr(diag_01, 1, 4))]
  emergency_admissions_by_diagnosis_site_lsoa_month[, diag_3char := substr(diag_4char, 1, 3)]
  emergency_admissions_by_diagnosis_site_lsoa_month[, diag_1char := substr(diag_4char, 1, 1)]
  emergency_admissions_by_diagnosis_site_lsoa_month[, diag_cause := toupper(substr(cause, 1, 3))]

  # Create integer age variable
  emergency_admissions_by_diagnosis_site_lsoa_month[, age := as.integer(startage)]
  emergency_admissions_by_diagnosis_site_lsoa_month[age >= 7001 & age <= 7007, age := 0]
  emergency_admissions_by_diagnosis_site_lsoa_month[age < 0 | age > 120, age := NA]

  # Hypoglycemia
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & (diag_3char %in% paste0("E", 10:15) | diag_4char %in% paste0("E", 161:162)), sub_measure := "hypoglycaemia"]

  # Acute mental health crisis
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_1char == "F", sub_measure := "acute mental health crisis"]

  # Epileptic fit
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_3char %in% paste0("G", 40:41), sub_measure := "epileptic fit"]

  # Angina
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_3char == "I20", sub_measure := "angina"]

  # DVT
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_3char %in% paste0("I", 80:82), sub_measure := "dvt"]

  # COPD
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_3char %in% paste0("J", 40:44), sub_measure := "copd"]

  # Cellulitis
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_3char == "L03", sub_measure := "cellulitis"]

  # Urinary tract infection
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_4char == "N390", sub_measure := "urinary tract infection"]

  # Non-specific chest pain
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_4char %in% paste0("R0", 72:74), sub_measure := "non-specific chest pain"]

  # Non-specific abdominal pain
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_3char == "R10", sub_measure := "non-specific abdominal pain"]

  # Pyrexial child (<6 years)
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_3char == "R50" & age < 6L, sub_measure := "pyrexial child (<6 years)"]

  # Minor head injuries
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_3char == "S00", sub_measure := "minor head injuries"]

  # Blocked catheter
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_4char == "T830", sub_measure := "blocked catheter"]

  # Falls (75+ years)
  falls_codes_digits <- expand.grid(d1 = 0:1, d2 = 0:9, stringsAsFactors = FALSE)
  falls_codes <- paste0("W", falls_codes_digits$d1, falls_codes_digits$d2)
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_cause %in% falls_codes & age >= 75L, sub_measure := "falls (75+ years)"]

  # collapse to attendances by (site, lsoa, month, avoidable condition)
  emergency_admissions_by_condition_site_lsoa_month <- emergency_admissions_by_diagnosis_site_lsoa_month[, .(value = sum(value)), by = .(procode, admimeth, lsoa, yearmonth, sub_measure)]

  # save data
  save(emergency_admissions_by_condition_site_lsoa_month, file = "data/emergency admissions by condition site lsoa month.Rda", compress = "xz")

  # collapse to attendances by (lsoa, month, avoidable condition)
  emergency_admissions_by_condition_lsoa_month <- emergency_admissions_by_condition_site_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth, sub_measure)]

  # Free up memory
  rm(emergency_admissions_by_diagnosis_site_lsoa_month, emergency_admissions_by_condition_site_lsoa_month)
  gc()

  # all emergency admissions
  emergency_admissions_by_lsoa_month <- emergency_admissions_by_condition_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth)]
  emergency_admissions_by_lsoa_month[, ':=' (measure = "all emergency admissions",
    sub_measure = as.character(NA))]

  # avoidable emergency admissions
  avoidable_emergency_admissions_by_condition_lsoa_month <- emergency_admissions_by_condition_lsoa_month[sub_measure != "other"]
  avoidable_emergency_admissions_by_condition_lsoa_month[, measure := "avoidable emergency admissions"]
  avoidable_emergency_admissions_by_lsoa_month <- avoidable_emergency_admissions_by_condition_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth, measure)]
  avoidable_emergency_admissions_by_lsoa_month[, sub_measure := "any"]
  avoidable_emergency_admissions_by_condition_lsoa_month <- rbind(avoidable_emergency_admissions_by_condition_lsoa_month, avoidable_emergency_admissions_by_lsoa_month)

  # format
  avoidable_emergency_admissions_measure <- fillDataPoints(avoidable_emergency_admissions_by_condition_lsoa_month)
  emergency_admissions_measure <- fillDataPoints(emergency_admissions_by_lsoa_month)

  # save
  save(avoidable_emergency_admissions_measure, file = "data/avoidable emergency admissions measure.Rda", compress = "bzip2")
  save(emergency_admissions_measure, file = "data/emergency admissions measure.Rda", compress = "bzip2")
}


save_length_of_stay_measure <- function() {

  db_conn <- connect2DB()

  tbl_name <- "emergency_admissions_by_diagnosis_site_lsoa_month"
  add_logic <- "epiorder = '1' AND (admimeth = '21' OR admimeth = '2A')"
  add_fields <- "admimeth, diag_01, diag_02, cause, startage"

  # Prepare query string to create temp table
  sql_create_tbl <- getSqlUpdateQuery("apc", tbl_name, add_logic, add_fields)

  # Takes ~2mins
  pc <- proc.time()
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_tbl)
  proc.time() - pc

  # retrieve data
  emergency_admissions_by_diagnosis_site_lsoa_month <- getDataFromTempTable(db_conn, tbl_name, "apc", add_fields)

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  ## Deal with error codes in the diag_01 field
  # cause code in primary diagnosis field
  emergency_admissions_by_diagnosis_site_lsoa_month[diag_01 == "R69X3", diag_01 := diag_02]

  ## categorise into avoidable conditions
  # default to 'other'
  emergency_admissions_by_diagnosis_site_lsoa_month[, sub_measure := "other"]

  # Create 1 character, 3 character and 4 character codes diagnosis codes and 3 character cause code
  emergency_admissions_by_diagnosis_site_lsoa_month[, diag_4char := toupper(substr(diag_01, 1, 4))]
  emergency_admissions_by_diagnosis_site_lsoa_month[, diag_3char := substr(diag_4char, 1, 3)]
  emergency_admissions_by_diagnosis_site_lsoa_month[, diag_1char := substr(diag_4char, 1, 1)]
  emergency_admissions_by_diagnosis_site_lsoa_month[, diag_cause := toupper(substr(cause, 1, 3))]

  # Create integer age variable
  emergency_admissions_by_diagnosis_site_lsoa_month[, age := as.integer(startage)]
  emergency_admissions_by_diagnosis_site_lsoa_month[age >= 7001 & age <= 7007, age := 0]
  emergency_admissions_by_diagnosis_site_lsoa_month[age < 0 | age > 120, age := NA]

  # Hypoglycemia
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & (diag_3char %in% paste0("E", 10:15) | diag_4char %in% paste0("E", 161:162)), sub_measure := "hypoglycaemia"]

  # Acute mental health crisis
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_1char == "F", sub_measure := "acute mental health crisis"]

  # Epileptic fit
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_3char %in% paste0("G", 40:41), sub_measure := "epileptic fit"]

  # Angina
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_3char == "I20", sub_measure := "angina"]

  # DVT
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_3char %in% paste0("I", 80:82), sub_measure := "dvt"]

  # COPD
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_3char %in% paste0("J", 40:44), sub_measure := "copd"]

  # Cellulitis
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_3char == "L03", sub_measure := "cellulitis"]

  # Urinary tract infection
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_4char == "N390", sub_measure := "urinary tract infection"]

  # Non-specific chest pain
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_4char %in% paste0("R0", 72:74), sub_measure := "non-specific chest pain"]

  # Non-specific abdominal pain
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_3char == "R10", sub_measure := "non-specific abdominal pain"]

  # Pyrexial child (<6 years)
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_3char == "R50" & age < 6L, sub_measure := "pyrexial child (<6 years)"]

  # Minor head injuries
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_3char == "S00", sub_measure := "minor head injuries"]

  # Blocked catheter
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_4char == "T830", sub_measure := "blocked catheter"]

  # Falls (75+ years)
  falls_codes_digits <- expand.grid(d1 = 0:1, d2 = 0:9, stringsAsFactors = FALSE)
  falls_codes <- paste0("W", falls_codes_digits$d1, falls_codes_digits$d2)
  emergency_admissions_by_diagnosis_site_lsoa_month[sub_measure == "other" & diag_cause %in% falls_codes & age >= 75L, sub_measure := "falls (75+ years)"]

  # collapse to attendances by (site, lsoa, month, avoidable condition)
  emergency_admissions_by_condition_site_lsoa_month <- emergency_admissions_by_diagnosis_site_lsoa_month[, .(value = sum(value)), by = .(procode, admimeth, lsoa, yearmonth, sub_measure)]

  # save data
  save(emergency_admissions_by_condition_site_lsoa_month, file = "data/emergency admissions by condition site lsoa month.Rda", compress = "xz")

  # collapse to attendances by (lsoa, month, avoidable condition)
  emergency_admissions_by_condition_lsoa_month <- emergency_admissions_by_condition_site_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth, sub_measure)]

  # Free up memory
  rm(emergency_admissions_by_diagnosis_site_lsoa_month, emergency_admissions_by_condition_site_lsoa_month)
  gc()

  # all emergency admissions
  emergency_admissions_by_lsoa_month <- emergency_admissions_by_condition_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth)]
  emergency_admissions_by_lsoa_month[, ':=' (measure = "all emergency admissions",
    sub_measure = as.character(NA))]

  # avoidable emergency admissions
  avoidable_emergency_admissions_by_condition_lsoa_month <- emergency_admissions_by_condition_lsoa_month[sub_measure != "other"]
  avoidable_emergency_admissions_by_condition_lsoa_month[, measure := "avoidable emergency admissions"]
  avoidable_emergency_admissions_by_lsoa_month <- avoidable_emergency_admissions_by_condition_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth, measure)]
  avoidable_emergency_admissions_by_lsoa_month[, sub_measure := "any"]
  avoidable_emergency_admissions_by_condition_lsoa_month <- rbind(avoidable_emergency_admissions_by_condition_lsoa_month, avoidable_emergency_admissions_by_lsoa_month)

  # format
  avoidable_emergency_admissions_measure <- fillDataPoints(avoidable_emergency_admissions_by_condition_lsoa_month)
  emergency_admissions_measure <- fillDataPoints(emergency_admissions_by_lsoa_month)

  # save
  save(avoidable_emergency_admissions_measure, file = "data/avoidable emergency admissions measure.Rda", compress = "bzip2")
  save(emergency_admissions_measure, file = "data/emergency admissions measure.Rda", compress = "bzip2")
}



#
# library(data.table)
# save_emergency_admissions_measure()
