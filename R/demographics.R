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

  # plot
  pyramid <- ggplot2::ggplot() +
    ggplot2::ggtitle(paste0("Population structure for ", locality, " (", ref_yr, ")\n[population: ", format(total_population, scientific = FALSE, big.mark=","), "]")) +
    ggplot2::geom_bar(data = population_data[sex == "female"], ggplot2::aes(x = age_cat, y = population, fill = sex), stat = "identity") +
    ggplot2::geom_bar(data = population_data[sex == "male"], ggplot2::aes(x = age_cat, y = population, fill = sex), stat = "identity") +
    ggplot2::geom_text(data = unique(population_data[, .(age_cat, age_lab = as.character(age_cat))]), ggplot2::aes(x = age_cat, y = 0, label = age_lab), colour = "white", fontface = "bold", stat = "identity") +
    ggplot2::scale_y_continuous(name = "Percentage of total population", limits = c(-4.5, 4.5)/100, expand = c(0, 0), breaks = -5:5/100, labels = function(x) { paste0(round(abs(x) * 100, 1), "%") }) +
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

getDemographics <- function(site, site_data, population_data, cm_data, cm_data_source, cm_period_length = 12, cm_threshold = 0) {

  # Pull up the sites details
  site_details <- site_data[unique_code == site]

  # Get year for which to get population data
  ref_year <- as.POSIXlt(site_details[, intervention_date])$year + 1900

  # Get catchment area LSOAs
  if(cm_data_source == "DfT") {
    lsoas <- cm_data[data_source == cm_data_source & frac_to_destination >= cm_threshold, lsoa]
    txt_to_pass <- paste0(site_details$town, " [by DfT]")
  } else {
    lsoas <- cm_data[data_source == cm_data_source & period_length == cm_period_length & frac_to_destination >= cm_threshold, lsoa]
    txt_to_pass <- paste0(site_details$town, " [by ", cm_data_source, " ", cm_period_length, "months]")
  }


  # Restrict population data to the years and LSOAs of our catchment area
  pop_data <- population_data[LSOA01 %in% lsoas & year == ref_year]

  # return plot
  return(getPopulationPyramid(pop_data, txt_to_pass))
}

load("data/site data.Rda")
load("data/catchment area sets.Rda")
load("data/2001-census lsoa annual population estimates 2006-2014.Rda")

sd <- copy(site_data)

out <- getDemographics("RCCSG", sd, LSOA2001_population_data_grouped, catchment_area_sets, "HES A&E")
suppressWarnings(print(out))
