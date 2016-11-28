
#Read in data
load("data/2001-census lsoa annual population estimates 2006-2014.Rda")
load("data/catchment area set final.Rda")
load("data/census data.Rda")
catchment_area_set_final[, year := as.integer(format(intervention_date, "%Y"))]


pop_all <- lsoa_population_annual_data[, .(population = sum(population)), by = .(LSOA01, year)]
pop_65plus <- lsoa_population_annual_data[age_cat %in% levels(lsoa_population_annual_data$age_cat)[15:20], .(population_65plus = sum(population)), by = .(LSOA01, year)]

lsoa_demographics <- merge(catchment_area_set_final[, .(lsoa, year, town, group, site_type)], pop_all, by.y = c("LSOA01", "year"), by.x = c("lsoa", "year"))
lsoa_demographics <- merge(lsoa_demographics, pop_65plus, by.y = c("LSOA01", "year"), by.x = c("lsoa", "year"))
lsoa_demographics <- merge(lsoa_demographics, census_data, by = "lsoa")

lsoa_demographics[, ':=' (non_white_pop = non_white_pc / 100 * population,
  longterm_illness_pop = longterm_illness_pc / 100 * population,
  imd_q1_pop = 0)]

lsoa_demographics[imd_quintile == 1, imd_q1_pop := population]

catchment_demographics <- lsoa_demographics[, .(population = sum(population),
  pop_65plus = sum(population_65plus),
  non_white_pop = sum(non_white_pop),
  longterm_illness_pop = sum(longterm_illness_pop),
  imd_q1_pop = sum(imd_q1_pop)), by = .(town, year, group, site_type)]

#catchment_demographics <- merge(catchment_demographics, getEDAttendancesInLastYear(), by = "town")
catchment_demographics <- merge(catchment_demographics, ed_attendances_by_site, by = "town")

catchment_demographics[, ':=' (population = round(population, -2),
  aged_65plus_pc = round(pop_65plus / population * 100, 1),
  non_white_pc = round(non_white_pop / population * 100, 1),
  longterm_illness_pc = round(longterm_illness_pop / population * 100, 1),
  imd_q1_pc = round(imd_q1_pop / population * 100, 1),
  annual_ed_attendances = round(annual_ed_attendances, -1),
  annual_ed_attendances_per1000pop = round(annual_ed_attendances / population * 1000, 0),
  pop_65plus = NULL,
  non_white_pop = NULL,
  longterm_illness_pop = NULL,
  imd_q1_pop = NULL)]


write.table(catchment_demographics, file = "clipboard", sep = "\t", row.names = FALSE)

catchment_populations_lsoa <- merge(lsoa_population_annual_data, catchment_area_set_final[, .(lsoa, year, town, group, site_type)], by.x = c("LSOA01", "year"), by.y = c("lsoa", "year"))
catchment_populations <- catchment_populations_lsoa[, .(population = sum(population)), by = .(sex, age_cat, town, group, site_type)]

# ggplot labeller (for presentational reasons)
blank <- function(x) {
  return("")
}


# Display age pyramids
displayAgePyramid <- function(pop_data) {
  data <- copy(pop_data)
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

displayAgePyramid(catchment_populations)



getEDAttendancesInLastYear <- function() {

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
  ed_attendances_by_site <- ed_attendances_by_site[relative_month > 12 & relative_month <= 24, .(annual_ed_attendances = sum(value)), by = town]

  return(ed_attendances_by_site)
}
