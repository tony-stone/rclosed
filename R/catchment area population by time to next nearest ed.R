library(data.table)
library(ggplot2)

load("data/catchment area set final.Rda")
load("data/2001-census lsoa annual population estimates 2006-2014.Rda")

intervention_sites <- catchment_area_set_final[site_type == "intervention", .(lsoa, town, group, diff_first_second, ae_post_intv, intervention_year = as.integer(format(intervention_date, "%Y")))]

intervention_sites_populations_lsoa <- merge(intervention_sites, lsoa_population_annual_data, by.x = c("lsoa", "intervention_year"), by.y = c("LSOA01", "year"), all.x = TRUE)

intervention_sites_populations_all <- intervention_sites_populations_lsoa[, .(population = sum(population)), by = .(town, group, diff_first_second)]

by_site_plot <- ggplot(intervention_sites_populations_all, aes(x = diff_first_second, weight = population)) +
  geom_histogram(aes(y = 1 * ..density..), position = 'identity', binwidth = 1) +
  scale_y_continuous(labels = scales::percent) +
  scale_x_continuous(breaks = 1:25, expand = c(0, 0.7)) +
  coord_cartesian(xlim = c(1, 25)) +
  labs(title = "Catchment area population by time to next nearest ED", x = "Time to next nearest ED\n(minutes)", y = "Catchment area population") +
  theme_bw() +
  facet_wrap(~group)


sites_combined_plot <- ggplot(intervention_sites_populations_all, aes(x = diff_first_second, weight = population)) +
  geom_histogram(aes(y = 1 * ..density..), position = 'identity', binwidth = 1) +
  scale_y_continuous(labels = scales::percent) +
  scale_x_continuous(breaks = 1:25, expand = c(0, 0.7)) +
  coord_cartesian(xlim = c(1, 25)) +
  labs(title = "All catchment areas population by time to next nearest ED", x = "Time to next nearest ED\n(minutes)", y = "Catchment areas population") +
  theme_bw()

ggsave("catchment area populations by time to next nearest ED - by site.png", by_site_plot, device = "png", path = "plots", width = 30, height = 20, units = "cm")
ggsave("catchment area populations by time to next nearest ED - all sites combined.png", sites_combined_plot, device = "png", path = "plots", width = 30, height = 20, units = "cm")

summary_stats_cheat <- intervention_sites_populations_all[, id := as.integer(row.names(intervention_sites_populations_all))][rep(id, population), .(town, group, diff_first_second)]

write.table(summary_stats_cheat[, .(mean = mean(diff_first_second), sd = sd(diff_first_second), q25 = quantile(diff_first_second, probs = 0.25, type = 8), median = quantile(diff_first_second, probs = 0.5, type = 8), q75 = quantile(diff_first_second, probs = 0.75, type = 8), q90 = quantile(diff_first_second, probs = .9, type = 8), q95 = quantile(diff_first_second, probs = .95, type = 8)), by = town],
  sep = "\t", file = "clipboard", row.names = FALSE)
write.table(summary_stats_cheat[, .(mean = mean(diff_first_second), sd = sd(diff_first_second), q25 = quantile(diff_first_second, probs = 0.25, type = 8), median = quantile(diff_first_second, probs = 0.5, type = 8), q75 = quantile(diff_first_second, probs = 0.75, type = 8), q90 = quantile(diff_first_second, probs = .9, type = 8), q95 = quantile(diff_first_second, probs = .95, type = 8))],
sep = "\t", file = "clipboard", row.names = FALSE)

