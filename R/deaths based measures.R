save_avoidable_deaths_measure <- function() {

  db_conn <- connect2DB()

  tbl_name <- "deaths_by_diagnosis_site_lsoa_month_dod"
  add_logic <- "cips_finished = TRUE AND nights_admitted < 184 AND died = TRUE AND date_of_death_last = last_disdate"
  add_fields <- "date_of_death_last, startage, endage, sex, diag_01, diag_02, cause, nights_admitted"

  # Prepare query string to create temp table
  sql_create_tbl <- getSqlUpdateQuery("apc", tbl_name, add_logic, add_fields)

  # Takes ~30s
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_tbl)

  # retrieve data
  hes_apc_deaths <- getDataFromTempTable(db_conn, tbl_name, "apc", add_fields)

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  hes_apc_deaths[startage > 90, startage := 90L]
  hes_apc_deaths[endage > 90, endage := 90L]
  hes_apc_deaths[, sex := as.character(sex)]
  hes_apc_deaths[sex == "1", sex := "male"]
  hes_apc_deaths[sex == "2", sex := "female"]

  hes_apc_deaths[, within_3days := FALSE]
  hes_apc_deaths[nights_admitted < 3, within_3days := TRUE]

  # Convert date of death to month of death
  hes_apc_deaths[, ':=' (month_of_death = as.Date(lubridate::fast_strptime(paste0(substr(date_of_death_last, 1, 7), "-01"), "%Y-%m-%d")),
    startage_cat = cut(startage, c(0, 1, seq(5, 95, 5)), right = FALSE),
    age_at_death = cut(endage, c(0, 1, seq(5, 95, 5)), right = FALSE))]

  # Slight mod to age_cat factor labels
  levels(hes_apc_deaths$age_at_death)[levels(hes_apc_deaths$age_at_death) == "[90,95)"] <- "[90,Inf)"
  levels(hes_apc_deaths$startage_cat)[levels(hes_apc_deaths$startage_cat) == "[90,95)"] <- "[90,Inf)"

  # classify avoidable deaths
  hes_apc_deaths <- classifyAvoidableDeaths(hes_apc_deaths)

  # collapse to attendances by (lsoa, month, avoidable condition)
  hes_apc_deaths_aggregated <- hes_apc_deaths[, .(value = sum(value)), by = .(lsoa, yearmonth, month_of_death, condition, startage_cat, age_at_death, sex, within_3days)]

  hes_apc_deaths_aggregated[, place_of_death := "nhs hospital"]
  setnames(hes_apc_deaths_aggregated, c("condition", "value"), c("sub_measure", "deaths_hes"))

  load("data/ons deaths (16 serious emergency conditions).Rda")
  setnames(deaths_serious_emergency_conditions, "deaths", "deaths_ons")

  stopifnot(levels(deaths_serious_emergency_conditions$age_cat) == levels(hes_apc_deaths_aggregated$age_at_death) & levels(deaths_serious_emergency_conditions$age_cat) == levels(hes_apc_deaths_aggregated$startage_cat))

  ons_hes_deaths <- merge(hes_apc_deaths_aggregated, deaths_serious_emergency_conditions, all = TRUE, by.x = c("lsoa", "sex", "age_at_death", "place_of_death", "sub_measure", "month_of_death"), by.y = c("lsoa", "sex", "age_cat", "place_of_death", "cause_of_death", "yearmonth"))






  # Deaths (within 3 days) crossing months
  bla1 <- ons_hes_deaths[yearmonth != month_of_death & within_3days == TRUE & sub_measure != "other"]

  # Missing deaths (well, just recorded as something else)
  bla2 <- ons_hes_deaths[(deaths_hes > deaths_ons | (!is.na(deaths_hes) & is.na(deaths_ons))) & within_3days == TRUE & sub_measure != "other"]

  # ONS NHS hospital deaths not in HES data
  bla3 <- ons_hes_deaths[(deaths_hes < deaths_ons | (!is.na(deaths_ons) & is.na(deaths_hes))) & place_of_death == "nhs hospital"  & sub_measure != "other"]

  bla3 <- ons_hes_deaths[!is.na(deaths_ons) & !is.na(deaths_hes)]


  # format
  emergency_admissions_measure <- fillDataPoints(emergency_admissions_by_lsoa_month)

  # Treat "all emergency admissions" as separate measure in same file
  emergency_admissions_measure[sub_measure == "all admissions", ':=' (measure = "all emergency admissions",
    sub_measure = as.character(NA))]

  # Collapse to site level
  emergency_admissions_site_measure <- collapseLsoas2Sites(emergency_admissions_measure)

  # save
  save(emergency_admissions_measure, file = createMeasureFilename(""), compress = "xz")
  save(emergency_admissions_site_measure, file = createMeasureFilename("", "site"), compress = "bzip2")
}




# ggplot(case_fatality_site_measure[town == "Newark" & sub_measure == "any"], aes(x = yearmonth, y = value, colour = town)) +
#   geom_line(size = 1) +
#   scale_x_date(name = "month", date_breaks = "3 months", date_labels = "%b %Y", expand = c(0, 15)) +
#   scale_y_continuous(labels = scales::comma, limits = c(0, NA), expand = c(0.02, 0)) +
#   theme(axis.text.x = element_text(face="bold", angle=90, hjust=0.0, vjust=0.3))

