library(data.table)

getPopulationPyramid <- function(population_data, locality) {

  # Get total population
  total_population <- as.integer(round(sum(population_data$population)))

  # Get year
  ref_yr <- population_data[1, year]

  # Collapse [0,1) and [1,5) age groups
  population_data[age_cat == "[0,1)", age_cat := "[1,5)"]
  levels(population_data$age_cat)[levels(population_data$age_cat) == "[1,5)"] <- "[0,5)"

  # Get population in each age_cat and sex as frac of total pop
  population_data <- population_data[, .(population = round(sum(population))), by = .(sex, age_cat)]
  population_data[, population := population / total_population]

  # For presentational reasons make male data negative
  population_data[sex == "male", population := -population]

  # round total population to nearest 100 for display in plot title
  total_population_rnd <- round(total_population, -2)

  # plot
  pyramid <- ggplot2::ggplot() +
    ggplot2::ggtitle(paste0("Population structure for ", locality, " (", ref_yr, ")\n[population: ", format(total_population_rnd, scientific = FALSE, big.mark=","), "]")) +
    ggplot2::geom_bar(data = population_data[sex == "female"], ggplot2::aes(x = age_cat, y = population, fill = sex), stat = "identity") +
    ggplot2::geom_bar(data = population_data[sex == "male"], ggplot2::aes(x = age_cat, y = population, fill = sex), stat = "identity") +
    ggplot2::geom_text(data = unique(population_data[, .(age_cat, age_lab = as.character(age_cat))]), ggplot2::aes(x = age_cat, y = 0, label = age_lab), colour = "white", fontface = "bold", stat = "identity") +
    ggplot2::scale_y_continuous(name = "Percentage of total population", limits = c(-5, 5)/100, expand = c(0, 0), breaks = -5:5/100, labels = function(x) { paste0(round(abs(x) * 100, 1), "%") }) +
    ggplot2::coord_flip() +
    ggplot2::scale_fill_manual(breaks=c("male","female"), values = c("#660099", "#ff6600")) +
    ggplot2::guides(fill = ggplot2::guide_legend(title = NULL, label.position = "top", keywidth = 2.5)) +
    ggplot2::theme(panel.background = ggplot2::element_blank(),
      axis.title.y = ggplot2::element_blank(),
      axis.text.y = ggplot2::element_blank(),
      axis.ticks.y = ggplot2::element_blank(),
      axis.line.y = ggplot2::element_blank(),
      panel.grid.major.y = ggplot2::element_blank(),
      panel.grid.minor.x = ggplot2::element_blank(),
      panel.grid.minor.y = ggplot2::element_blank(),
      panel.ontop = TRUE,
      legend.position = "top",
      legend.text = ggplot2::element_text(colour = "#a0a0a0", size = 10, face = 'bold'),
      plot.title = ggplot2::element_text(colour = "#808080", size = 16, face = 'bold'))

  return(pyramid)
}


getCensusDemographics <- function(pop_data_2011) {
  # read census data
  load("data/census data.Rda")

  # Collapse population data to LSOA level (i.e. collpase age_cat and sex)
  pop_data_2011_summed <- pop_data_2011[, .(population = sum(population)), by = .(LSOA01)]

  # Merge census data into relevant population data
  census_demographics <- merge(pop_data_2011_summed, census_data, by.x = "LSOA01", by.y = "lsoa", all.x = TRUE)

  # Calculate longterm ill and non-whites in catchment area in absolute numbers at LSOA level
  census_demographics[, ':=' (longterm_illness = longterm_illness_pc * population,
    non_white = non_white_pc * population)]

  # And as a percentage at catchment area level
  return_data <- census_demographics[, .(longterm_illness_pc = sum(longterm_illness) / sum(population), non_white_pc = sum(non_white) / sum(population))][1, ]

  # Population in top quintile of deprivation as percentage at catchment area level
  imd_q1_population <- sum(census_demographics[imd_quintile == 1, population]) / sum(census_demographics$population) * 100

  # Add IMD Q1 data
  return_data[, imd_q1_pc := imd_q1_population]

  return(return_data)
}


getDemographics <- function(site, catchment_area_set, site_info, pop_data_all, attendance_data) {

  lsoas <- catchment_area_set[unique_code == site, lsoa]
  intv_date <- site_info[unique_code == site, intervention_date]
  start_date_inc <- intv_date
  lubridate::month(start_date_inc) <- lubridate::month(start_date_inc) - 12L

  ref_year <- as.POSIXlt(intv_date)$year + 1900
  pop_data <- pop_data_all[LSOA01 %in% lsoas & year == ref_year]

  # Get total population
  total_pop <- as.integer(round(sum(pop_data$population)))

  # Get total population rounded to hundreds
  total_pop_rnd <- round(total_pop, -2)

  # Get total population aged 65+ as percentage of total population
  pop_65plus_pc <- sum(pop_data[age_cat %in% c("[65,70)", "[70,75)", "[75,80)", "[80,85)", "[85,90)", "[90,Inf)"), population]) / total_pop * 100

  # Calculate total attendances in year prior to closure from all LSOAs in catchment area to any provider
  ae_atnd <- attendance_data[lsoa %in% lsoas & yearmonth >= start_date_inc & yearmonth < intv_date, sum(attendances)] / total_pop * 1000

  # Get non whites, longterm ill and imd
  catchment_demography <- getCensusDemographics(pop_data_all[year == 2011 & LSOA01 %in% lsoas])

  catchment_demography[, ':=' (unique_code = site,
    total_population = total_pop_rnd,
    population_65plus_pc = pop_65plus_pc,
    ae_attendances = ae_atnd)]

  return(catchment_demography)
}


# main --------------------------------------------------------------------

createDemographics <- function() {
  # load data
  load("data/site data.Rda")
  load("data/catchment area set final.Rda")
  load("data/attendances by trust lsoa month.Rda")
  load("data/2001-census lsoa annual population estimates 2006-2014.Rda")


  # Pull up the sites details
  sites <- site_data$unique_code

  demographics_list <- lapply(sites, getDemographics, catchment_area_set = catchment_area_set_final, site_info = site_data, pop_data_all = lsoa_population_annual_data, attendance_data = attendances_by_trust_lsoa_month)

  site_demographics <-  rbindlist(demographics_list)

  setcolorder(site_demographics, c("unique_code", "total_population", "population_65plus_pc", "non_white_pc", "imd_q1_pc", "longterm_illness_pc", "ae_attendances"))

  site_demographics <- merge(site_data[, .(unique_code, town, group, intervention_date, is_intervention)], site_demographics, by = "unique_code")

  site_demographics[, ':=' (population_65plus_pc = round(population_65plus_pc, 1),
    non_white_pc = round(non_white_pc, 1),
    imd_q1_pc = round(imd_q1_pc, 1),
    longterm_illness_pc = round(longterm_illness_pc, 1),
    ae_attendances = round(ae_attendances))]

  setorderv(site_demographics, c("intervention_date", "is_intervention", "town"), c(1, -1, 1))

  write.table(site_demographics, "clipboard", sep = "\t", row.names = FALSE)
  save(site_demographics, file = "D:/site demographics.Rda")

# population pyramid plots
  pop_plots <- lapply(sites, function(site) {
    getPopulationPyramid(lsoa_population_annual_data[LSOA01 %in% catchment_area_set_final[unique_code == site, lsoa] &
        year == as.POSIXlt(site_data[unique_code == site, intervention_date])$year + 1900], site_data[unique_code == site, town])
  })
  for(i in 1:length(pop_plots)) {
    suppressWarnings(ggplot2::ggsave(paste0(site_data[unique_code == sites[i], town], " population pyramid.jpg"), plot = pop_plots[[i]], device = "jpeg", path = "D:/"))
  }
}


createDemographics()
