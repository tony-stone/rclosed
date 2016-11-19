db_conn <- connect2DB()

tbl_name <- "emergency_admissions_by_diagnosis_site_lsoa_month"
add_logic <- ""
add_fields <- "startage, diag_01, diag_02, cause"

# Prepare query string to create temp table
sql_create_tbl <- getSqlUpdateQuery("apc", tbl_name, add_logic, add_fields)

# Takes ~30s
resource <- RJDBC::dbSendUpdate(db_conn, sql_create_tbl)

# retrieve data
emergency_admissions_by_diagnosis_site_lsoa_month <- getDataFromTempTable(db_conn, tbl_name, "apc", add_fields)

# Disconnect from DB
DBI::dbDisconnect(db_conn)
db_conn <- NULL

# Classify
emergency_admissions_by_ucc_site_lsoa_month <- classifyAvoidableAdmissions(emergency_admissions_by_diagnosis_site_lsoa_month)
emergency_admissions_by_sec_site_lsoa_month <-classifyAvoidableDeaths(emergency_admissions_by_diagnosis_site_lsoa_month)

# collapse to attendances by (lsoa, month, avoidable condition)
emergency_admissions_by_ucc_lsoa_month <- emergency_admissions_by_ucc_site_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth, condition)]
emergency_admissions_by_sec_lsoa_month <- emergency_admissions_by_sec_site_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth, condition)]

# format
setnames(emergency_admissions_by_ucc_lsoa_month, "condition", "sub_measure")
setnames(emergency_admissions_by_sec_lsoa_month, "condition", "sub_measure")

# Free up memory
rm(emergency_admissions_by_ucc_site_lsoa_month, emergency_admissions_by_sec_site_lsoa_month)
gc()

# avoidable emergency admissions
emergency_admissions_by_ucc_lsoa_month[, measure := "UCC emergency admission"]
emergency_admissions_by_sec_lsoa_month[, measure := "SEC emergency admissions"]


# format
emergency_admissions_by_ucc_lsoa_month <- fillDataPoints(emergency_admissions_by_ucc_lsoa_month)
emergency_admissions_by_sec_lsoa_month <- fillDataPoints(emergency_admissions_by_sec_lsoa_month)

# Collapse to site level
emergency_admissions_by_ucc_month <- collapseLsoas2Sites(emergency_admissions_by_ucc_lsoa_month)
emergency_admissions_by_sec_month <- collapseLsoas2Sites(emergency_admissions_by_sec_lsoa_month)


period_label <- c("period1_before", "period2_after")
emergency_admissions_by_ucc_month[, period := period_label[as.integer(relative_month > 24) + 1]]
emergency_admissions_by_sec_month[, period := period_label[as.integer(relative_month > 24) + 1]]

# Collapse to before/after
emergency_admissions_by_ucc_period <- emergency_admissions_by_ucc_month[, .(value = sum(value, na.rm = TRUE)), by = .(town, period, sub_measure)]
emergency_admissions_by_sec_period <- emergency_admissions_by_sec_month[, .(value = sum(value, na.rm = TRUE)), by = .(town, period, sub_measure)]
emergency_admissions_by_period <- emergency_admissions_by_ucc_period[, .(value = sum(value, na.rm = TRUE)), by = .(town, period)]


emergency_admissions_by_ucc_period_wide <- dcast(emergency_admissions_by_ucc_period, sub_measure ~ town + period, value.var = "value")
emergency_admissions_by_sec_period_wide <- dcast(emergency_admissions_by_sec_period, sub_measure ~ town + period, value.var = "value")

write.table(emergency_admissions_by_ucc_period_wide, file = "clipboard", sep = "\t", row.names = FALSE)
write.table(emergency_admissions_by_sec_period_wide, file = "clipboard", sep = "\t", row.names = FALSE)

