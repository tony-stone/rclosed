
catchment_demographics <- getCatchmentDemographics(0)

write.table(catchment_demographics, file = "clipboard", sep = "\t", row.names = FALSE)
write.table(catchment_demographics[site_type %in% c("intervention", "matched control")], file = "clipboard", sep = "\t", row.names = FALSE)

displayAgePyramids(0)

# saveCatchmentAgeSex()
# saveCatchmentDemographics()
# saveCatchmentMonthlyAttendances()
# saveCatchmentMonthlyDeaths()



getCatchmentDemographics <- function(year_relative_to_intervention = 0L) {

  load("data/demographics/catchment area demographics by year.Rda")
  load("data/demographics/catchment area ED attendances by month.Rda")
  load("data/demographics/catchment area deaths by month.Rda")

  min_month <- max(c(13 + year_relative_to_intervention * 12, 2))

  attendances_deaths_by_catchment_area_month <- merge(ed_attendances_by_catchment_area_month, deaths_by_catchment_area_month, by = c("town", "relative_month"))
  catchment_attendances_deaths <- attendances_deaths_by_catchment_area_month[relative_month >= min_month & relative_month < min_month + 12L, .(annual_ed_attendances = sum(ed_attendances), annual_deaths = sum(all_deaths), annual_SEC_deaths = sum(SEC_deaths)), by = .(town)]

  catchment_demographics_raw <- merge(catchment_demographics_raw[demographics_year == (year_of_closure + year_relative_to_intervention)], catchment_attendances_deaths, by = "town")

  catchment_demographics <- catchment_demographics_raw[, .(demographics_year,
    town,
    year_of_closure,
    group,
    site_type,
    population = round(population, -2),
    aged_65plus_pc = round(pop_65plus / population * 100, 1),
    most_deprived_quintile_pc = round(imd_q1_pop / population * 100, 1),
    rural_pc = round(rural_pop / population * 100, 1),
    non_white_pc = round(non_white_pop / population * 100, 1),
    longterm_illness_pc = round(longterm_illness_pop / population * 100, 1),
    #  annual_ed_attendances = round(annual_ed_attendances, -1),
    annual_ed_attendances_per1000 = round(annual_ed_attendances / population * 1000, 0),
    #  annual_deaths = round(annual_deaths, -1),
    annual_deaths_per100k = round(annual_deaths / population * 100000, -1),
    #  annual_SEC_deaths = round(annual_SEC_deaths, -1),
    annual_SEC_deaths_per100k = round(annual_SEC_deaths / population * 100000, -1),
    SEC_deaths_pc = round(annual_SEC_deaths / annual_deaths * 100, 1)
  )]

  setorder(catchment_demographics, group, site_type)

  return(catchment_demographics)
}


# ggplot labeller (for presentational reasons)
blank <- function(x) {
  return("")
}


# Display age pyramids
displayAgePyramids <- function(relative_year = 0L) {

  load("data/demographics/catchment area age sex demographics by year.Rda")

  data <- catchment_populations[year == year_of_closure + relative_year]

  data[, pop_density := population / sum(population), by = .(sex, town, group, site_type)]

  plot <- ggplot2::ggplot(data, ggplot2::aes(x = age_cat, y = pop_density, fill = sex)) +
    ggplot2::labs(title = "population pyramids by site (year of closure)", x = "Age", y = "percentage of population (within sex)") +
    ggplot2::geom_bar(data = data[sex == "female"], stat="identity") +
    ggplot2::geom_bar(data = data[sex == "male"], stat="identity", ggplot2::aes(y = pop_density * -1)) +
    ggplot2::scale_y_continuous(breaks = seq(-100, 100, 25) / 1000, labels = paste0(abs(seq(-100, 100, 25) / 10), "%")) +
    ggplot2::coord_flip() +
    ggplot2::facet_wrap(~ group + site_type + town, ncol = 4, shrink = TRUE, as.table = TRUE, drop = TRUE, labeller = ggplot2::labeller(group = blank, site_type = blank, .multi_line = FALSE))

  suppressWarnings(print(plot))
}


saveCatchmentAgeSex <- function() {
  load("data/2001-census lsoa annual population estimates 2006-2014.Rda")
  load("data/catchment area set final.Rda")

  catchment_area_set_final[, year_of_closure := as.integer(format(intervention_date, "%Y"))]
  catchment_populations_lsoa <- merge(lsoa_population_annual_data, catchment_area_set_final[, .(lsoa, year_of_closure, town, group, site_type)], by.x = "LSOA01", by.y = "lsoa")

  catchment_populations <- catchment_populations_lsoa[, .(population = sum(population)), by = .(sex, age_cat, year, town, group, site_type, year_of_closure)]

  save(catchment_populations, file = "data/demographics/catchment area age sex demographics by year.Rda")
}


saveCatchmentDemographics <- function() {
  #Read in data
  load("data/2001-census lsoa annual population estimates 2006-2014.Rda")
  load("data/catchment area set final.Rda")
  load("data/census data.Rda")

  catchment_area_set_final[, year_of_closure := as.integer(format(intervention_date, "%Y"))]

  pop_all <- lsoa_population_annual_data[, .(population = sum(population)), by = .(LSOA01, year)]
  pop_65plus <- lsoa_population_annual_data[age_cat %in% levels(lsoa_population_annual_data$age_cat)[15:20], .(population_65plus = sum(population)), by = .(LSOA01, year)]

  lsoa_demographics <- merge(catchment_area_set_final[, .(lsoa, year_of_closure, town, group, site_type)], pop_all, by.x = "lsoa", by.y = "LSOA01")
  lsoa_demographics <- merge(lsoa_demographics, pop_65plus, by.x = c("lsoa", "year"), by.y = c("LSOA01", "year"))
  lsoa_demographics <- merge(lsoa_demographics, census_data, by = "lsoa")

  lsoa_demographics[, ':=' (non_white_pop = non_white_pc / 100 * population,
    longterm_illness_pop = longterm_illness_pc / 100 * population,
    imd_q1_pop = 0,
    rural_pop = 0)]

  lsoa_demographics[imd_quintile == 1, imd_q1_pop := population]
  lsoa_demographics[ruc_2level == "Rural", rural_pop := population]

  catchment_demographics_raw <- lsoa_demographics[, .(population = sum(population),
    pop_65plus = sum(population_65plus),
    non_white_pop = sum(non_white_pop),
    longterm_illness_pop = sum(longterm_illness_pop),
    imd_q1_pop = sum(imd_q1_pop),
    rural_pop = sum(rural_pop)),
    by = .(town, year_of_closure, year, group, site_type)]

  setnames(catchment_demographics_raw, "year", "demographics_year")

  save(catchment_demographics_raw, file = "data/demographics/catchment area demographics by year.Rda")
}


saveCatchmentMonthlyAttendances <- function() {

  db_conn <- connect2DB()

  tbl_name <- "attendances_by_mode_trust_lsoa_month"
  add_logic <- "aeattendcat = '1'"
  add_fields <- ""

  # Prepare query string to create temp table
  sql_create_tbl <- getSqlUpdateQuery("ae", tbl_name, add_logic, add_fields)

  # Takes ~1.5mins
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_tbl)

  # retrieve data
  ed_attendances_by_mode_trust_lsoa_month <- getDataFromTempTable(db_conn, tbl_name, "ae", add_fields)

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  # collapse to attendances by (lsoa, month)
  ed_attendances_by_lsoa_month <- ed_attendances_by_mode_trust_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth)]

  # format
  ed_attendances_by_lsoa_month[, ':=' (measure = "ed attendances",
    sub_measure = "any")]

  ed_attendances_by_site <- collapseLsoas2Sites(fillDataPoints(ed_attendances_by_lsoa_month))

  ed_attendances_by_catchment_area_month <- ed_attendances_by_site[, .(town, relative_month, ed_attendances = value)]

  save(ed_attendances_by_catchment_area_month, file = "data/demographics/catchment area ED attendances by month.Rda")
}


saveCatchmentMonthlyDeaths <- function(relative_year) {
  load("data/hes linked mortality.Rda")

  ## Make long form and classify causes of deaths as one of the SECs
  ons_deaths_long <- melt(HES_linked_mortality, id.vars = c("encrypted_hesid", "startage", "sex", "date_of_death", "lsoa"), na.rm = TRUE, variable.factor = FALSE, variable.name = "condition_rank", value.name = "diag_01")
  ons_deaths_long[, ':=' (diag_02 = NA,
    cause = diag_01,
    condition_rank = as.integer(substr(condition_rank, 12, 13)))]

  ons_deaths_long <- classifyAvoidableDeaths(ons_deaths_long)

  ons_deaths_long[condition %in% c("road traffic accident", "falls", "self harm"), condition_rank := condition_rank + 16L]
  ons_deaths_long[condition == "other", condition_rank := condition_rank + 32L]

  ons_deaths <- ons_deaths_long[, min_rank := min(condition_rank), by = encrypted_hesid][condition_rank == min_rank, .(encrypted_hesid, date_of_death, lsoa, condition)]

  ons_deaths[, yearmonth := as.Date(lubridate::fast_strptime(paste0(substr(date_of_death, 1, 7), "-01"), format = "%Y-%m-%d", lt = FALSE))]

  ons_sec_deaths <- ons_deaths[condition != "other", .(value = .N), by = .(yearmonth, lsoa)]
  ons_sec_deaths[, sub_measure := "SEC_deaths"]

  ons_all_deaths <- ons_deaths[, .(value = .N), by = .(yearmonth, lsoa)]
  ons_all_deaths[, sub_measure := "all_deaths"]

  ons_sec_all_deaths <- rbind(ons_all_deaths, ons_sec_deaths)
  ons_sec_all_deaths[, measure := "all deaths"]

  ons_sec_all_deaths <- collapseLsoas2Sites(fillDataPoints(ons_sec_all_deaths))

  deaths_by_catchment_area_month <- dcast(ons_sec_all_deaths, town + relative_month ~ sub_measure, value.var = "value")

  save(deaths_by_catchment_area_month, file = "data/demographics/catchment area deaths by month.Rda")
}
