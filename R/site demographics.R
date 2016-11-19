
#Read in data
load("D:/Rpackages/rclosed/data/2001-census lsoa annual population estimates 2006-2014.Rda")
load("D:/Rpackages/rclosed/data/catchment area set final.Rda")
catchment_area_set_final[, year := as.integer(format(intervention_date, "%Y"))]
catchment_populations_lsoa <- merge(lsoa_population_annual_data, catchment_area_set_final[, .(lsoa, year, town, group, site_type)], by.x = c("LSOA01", "year"), by.y = c("lsoa", "year"))
catchment_populations <- catchment_populations_lsoa[, .(population = sum(population)), by = .(sex, age_cat, town, group, site_type)]

# ggplot labeller (for presentational reasons)
blank <- function(x) {
  return("")
}


# Display age pyramids for ambulance services
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
