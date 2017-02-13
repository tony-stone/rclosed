library(data.table)
library(ggplot2)



# HES A&E -----------------------------------------------------------------

db_conn <- connect2DB()

tbl_name <- "attendances_by_trust_lsoa_month"
add_logic <- ""
add_fields <- ""

# Prepare query string to create temp table
sql_create_tbl <- getSqlUpdateQuery("ae", tbl_name, add_logic, add_fields)

# Takes ~1min
resource <- RJDBC::dbSendUpdate(db_conn, sql_create_tbl)

# retrieve data
ed_attendances_by_trust_lsoa_month <- getDataFromTempTable(db_conn, tbl_name, "ae", add_fields)

# Disconnect from DB
DBI::dbDisconnect(db_conn)
db_conn <- NULL

# collapse to attendances by (lsoa, month, mode of arrival)
ed_attendances_by_lsoa_month <- ed_attendances_by_trust_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth)]

ed_attendances_by_lsoa_month[, ':=' (measure = "ed attendances",
  sub_measure = "all")]

# format
ed_attendances_by_lsoa_month_formatted <- fillDataPoints(ed_attendances_by_lsoa_month, crop = FALSE)

# Collapse to site level
ed_attendances_by_site_month <- collapseLsoas2Sites(ed_attendances_by_lsoa_month_formatted)

# add group town
town_group_lookup <- unique(ed_attendances_by_site_month[site_type == "intervention", .(group, group_town = town)], by = c("group_town", "group"))
ed_attendances_by_site_month <- merge(ed_attendances_by_site_month, town_group_lookup, by = "group")

time_periods_long <- unique(ed_attendances_by_site_month[relative_month == 1 | relative_month == 48, .(relative_month, group_town, yearmonth)])
var_names <- c("start", rep(NA, 46), "end")
time_periods_long[, relative_month := var_names[relative_month]]
time_periods <- dcast(time_periods_long, group_town ~ relative_month, value.var = "yearmonth")
time_periods[, intv := start + floor((end - start) / 2)]
time_periods[start < as.Date("2007-04-01"), start := as.Date("2007-04-01")]


# plot
all_ae_attd_plot <- ggplot(ed_attendances_by_site_month[site_type != "pooled control" & sub_measure == "all"], aes(x = yearmonth, y = value, linetype = site_type)) +
  scale_x_date(name = "month", date_breaks = "3 months", date_labels = "%b %Y", expand = c(0, 15)) + #scale_x_continuous(breaks = 1:48, limits = c(1, 48), expand = c(0, 0.5))
  scale_y_continuous(labels = scales::comma, limits = c(0, NA)) +
  scale_linetype_discrete(name = guide_legend("site type")) +
  geom_line(size = 1) +
  facet_wrap(~ group_town, ncol = 2, scales = "fixed", shrink = TRUE, as.table = TRUE, drop = TRUE) +
  theme(axis.text.x = element_text(face="bold", angle=90, hjust=0.0, vjust=0.3)) +
  labs(title = "Monthly HES A&E attendances", y = "attendances", x = "month") +
  geom_rect(data = time_periods, aes(xmax = start), xmin = -Inf, ymin = -Inf, ymax = Inf, fill = "black", alpha = 0.5, inherit.aes = FALSE) +
  geom_rect(data = time_periods, aes(xmin = end), xmax = Inf, ymin = -Inf, ymax = Inf, fill = "black", alpha = 0.5, inherit.aes = FALSE) +
  geom_vline(data = time_periods, aes(xintercept = as.double(intv)))

ggsave("all HES AE attendances by site.jpg", all_ae_attd_plot, path = "plots/data quality/", width = 30, height = 20, units = "cm")



# HES APC -----------------------------------------------------------------

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
emergency_admissions_by_ucc_lsoa_month <- fillDataPoints(emergency_admissions_by_ucc_lsoa_month, crop = FALSE)
emergency_admissions_by_sec_lsoa_month <- fillDataPoints(emergency_admissions_by_sec_lsoa_month, crop = FALSE)

# Collapse to site level
emergency_admissions_by_ucc_month <- collapseLsoas2Sites(emergency_admissions_by_ucc_lsoa_month)
emergency_admissions_by_sec_month <- collapseLsoas2Sites(emergency_admissions_by_sec_lsoa_month)

# add new group_town var
town_group_lookup <- unique(emergency_admissions_by_ucc_month[site_type == "intervention", .(group, group_town = town)], by = c("group_town", "group"))
emergency_admissions_by_ucc_month <- merge(emergency_admissions_by_ucc_month, town_group_lookup, by = "group")
emergency_admissions_by_sec_month <- merge(emergency_admissions_by_sec_month, town_group_lookup, by = "group")

all_adm <- emergency_admissions_by_sec_month[, .(value = sum(value)), by = .(yearmonth, measure, town, group, group_town, site_type, relative_month)]
all_adm[, sub_measure := "all"]


all_em_adms_plot <- ggplot(all_adm[site_type != "pooled control" & sub_measure == "all"], aes(x = yearmonth, y = value, linetype = site_type)) +
  scale_x_date(name = "month", date_breaks = "3 months", date_labels = "%b %Y", expand = c(0, 15)) +
  scale_y_continuous(labels = scales::comma, limits = c(0, NA)) +
  scale_linetype_discrete(name = guide_legend("site type")) +
  geom_line(size = 1) +
  facet_wrap(~ group_town, ncol = 2, scales = "fixed", shrink = TRUE, as.table = TRUE, drop = TRUE) +
  theme(axis.text.x = element_text(face="bold", angle=90, hjust=0.0, vjust=0.3)) +
  labs(title = "Monthly emergency admissions for all conditions", y = "admissions", x = "month") +
  geom_rect(data = time_periods, aes(xmax = start), xmin = -Inf, ymin = -Inf, ymax = Inf, fill = "black", alpha = 0.5, inherit.aes = FALSE) +
  geom_rect(data = time_periods, aes(xmin = end), xmax = Inf, ymin = -Inf, ymax = Inf, fill = "black", alpha = 0.5, inherit.aes = FALSE) +
  geom_vline(data = time_periods, aes(xintercept = as.double(intv)))


plotStuff <- function(conditions_data, title, time_periods) {
  data <- copy(conditions_data)

  any_ucc_adm <- data[sub_measure != "other", .(value = sum(value)), by = .(yearmonth, measure, town, group, group_town, site_type, relative_month)]
  any_ucc_adm[, sub_measure := "any"]

  data <- rbind(data, all_adm, any_ucc_adm)
  data[sub_measure == "any" | sub_measure == "other", value_pc := value / sum(value), by = .(yearmonth, town)]
  data[sub_measure != "any" & sub_measure != "other", value_pc := value / sum(value), by = .(yearmonth, town)]

  plot <- ggplot(data[site_type != "pooled control" & sub_measure == "any"], aes(x = yearmonth, y = value_pc, linetype = site_type)) +
    scale_x_date(name = "month", date_breaks = "3 months", date_labels = "%b %Y", expand = c(0, 15)) +
    scale_y_continuous(labels = scales::percent, limits = c(0, NA)) +
    scale_linetype_discrete(name = guide_legend("site type")) +
    geom_line(size = 1) +
    facet_wrap(~ group_town, ncol = 2, scales = "fixed", shrink = TRUE, as.table = TRUE, drop = TRUE) +
    theme(axis.text.x = element_text(face="bold", angle=90, hjust=0.0, vjust=0.3)) +
    labs(title = paste("Monthly emergency admissions for any", title, "condition \n(as percentage of all emergency admissions)"), y = "admissions", x = "relative month") +
    geom_rect(data = time_periods, aes(xmax = start), xmin = -Inf, ymin = -Inf, ymax = Inf, fill = "black", alpha = 0.5, inherit.aes = FALSE) +
    geom_rect(data = time_periods, aes(xmin = end), xmax = Inf, ymin = -Inf, ymax = Inf, fill = "black", alpha = 0.5, inherit.aes = FALSE) +
    geom_vline(data = time_periods, aes(xintercept = as.double(intv)))


  return(plot)
}

all_ucc_em_adms_plot <- plotStuff(emergency_admissions_by_ucc_month, "urgent care", time_periods)
all_sec_em_adms_plot <- plotStuff(emergency_admissions_by_sec_month, "serious, emergency", time_periods)

ggsave("all emergency admissions by site.jpg", all_em_adms_plot, path = "plots/data quality/", width = 30, height = 20, units = "cm")
ggsave("all ucc emergency admissions by site.jpg", all_ucc_em_adms_plot, path = "plots/data quality/", width = 30, height = 20, units = "cm")
ggsave("all sec emergency admissions by site.jpg", all_sec_em_adms_plot, path = "plots/data quality/", width = 30, height = 20, units = "cm")




# ONS/HES Deaths ----------------------------------------------------------

# Load death data
load("data/hes linked mortality.Rda")

## Make long form and classify causes of deaths as one of the SECs
ons_deaths_long <- melt(HES_linked_mortality, id.vars = c("encrypted_hesid", "startage", "sex", "date_of_death", "lsoa"), na.rm = TRUE, variable.factor = FALSE, variable.name = "condition_rank", value.name = "diag_01")
ons_deaths_long[, ':=' (diag_02 = NA,
  cause = diag_01,
  condition_rank = as.integer(substr(condition_rank, 12, 13)))]

ons_deaths_long <- classifyAvoidableDeaths(ons_deaths_long)

ons_deaths_long[condition %in% c("road traffic accident", "falls", "self harm"), condition_rank := condition_rank + 16L]
ons_deaths_long[condition == "other", condition_rank := condition_rank + 32L]

ons_deaths <- ons_deaths_long[, min_rank := min(condition_rank), by = encrypted_hesid][condition_rank == min_rank, .(date_of_death, lsoa, condition)]

ons_deaths[, ':=' (yearmonth = as.Date(lubridate::fast_strptime(paste0(substr(date_of_death, 1, 7), "-01"), format = "%Y-%m-%d", lt = FALSE)),
  date_of_death = NULL)]

setnames(ons_deaths, "condition", "sub_measure")

ons_deaths_summary <- ons_deaths[, .(measure = "deaths", value = .N), by = .(yearmonth, lsoa, sub_measure)]

# format
ons_deaths_by_cond_lsoa_month <- fillDataPoints(ons_deaths_summary, crop = FALSE)

# Collapse to site level
ons_deaths_by_cond_month <- collapseLsoas2Sites(ons_deaths_by_cond_lsoa_month)

# add new group_town var
town_group_lookup <- unique(ons_deaths_by_cond_month[site_type == "intervention", .(group, group_town = town)], by = c("group_town", "group"))
ons_deaths_by_cond_month <- merge(ons_deaths_by_cond_month, town_group_lookup, by = "group")

all_deaths <- ons_deaths_by_cond_month[, .(value = sum(value)), by = .(yearmonth, measure, town, group, group_town, site_type, relative_month)]
all_deaths[, sub_measure := "all"]

all_deaths_plot <- ggplot(all_deaths[site_type != "pooled control" & sub_measure == "all"], aes(x = yearmonth, y = value, linetype = site_type)) +
  scale_x_date(name = "month", date_breaks = "3 months", date_labels = "%b %Y", expand = c(0, 15)) +
  scale_y_continuous(labels = scales::comma, limits = c(0, NA)) +
  scale_linetype_discrete(name = guide_legend("site type")) +
  geom_line(size = 1) +
  facet_wrap(~ group_town, ncol = 2, scales = "fixed", shrink = TRUE, as.table = TRUE, drop = TRUE) +
  theme(axis.text.x = element_text(face="bold", angle=90, hjust=0.0, vjust=0.3)) +
  labs(title = "Monthly deaths from all causes", y = "deaths", x = "month") +
  geom_rect(data = time_periods, aes(xmax = start), xmin = -Inf, ymin = -Inf, ymax = Inf, fill = "black", alpha = 0.5, inherit.aes = FALSE) +
  geom_rect(data = time_periods, aes(xmin = end), xmax = Inf, ymin = -Inf, ymax = Inf, fill = "black", alpha = 0.5, inherit.aes = FALSE) +
  geom_vline(data = time_periods, aes(xintercept = as.double(intv)))

data <- copy(ons_deaths_by_cond_month)

any_sec_death <- data[sub_measure != "other", .(value = sum(value)), by = .(yearmonth, measure, town, group, group_town, site_type, relative_month)]
any_sec_death[, sub_measure := "any"]

data <- rbind(data, any_sec_death)
data[sub_measure == "any" | sub_measure == "other", value_pc := value / sum(value), by = .(yearmonth, town)]
data[sub_measure != "any" & sub_measure != "other", value_pc := value / sum(value), by = .(yearmonth, town)]


sec_deaths_plot <- ggplot(data[site_type != "pooled control" & sub_measure == "any"], aes(x = yearmonth, y = value_pc, linetype = site_type)) +
  scale_x_date(name = "month", date_breaks = "3 months", date_labels = "%b %Y", expand = c(0, 15)) +
  scale_y_continuous(labels = scales::percent, limits = c(0, NA)) +
  scale_linetype_discrete(name = guide_legend("site type")) +
  geom_line(size = 1) +
  facet_wrap(~ group_town, ncol = 2, scales = "fixed", shrink = TRUE, as.table = TRUE, drop = TRUE) +
  theme(axis.text.x = element_text(face="bold", angle=90, hjust=0.0, vjust=0.3)) +
  labs(title = "Monthly deaths from any serious, emergency condition \n(as percentage of all deaths)", y = "deaths", x = "month") +
  geom_rect(data = time_periods, aes(xmax = start), xmin = -Inf, ymin = -Inf, ymax = Inf, fill = "black", alpha = 0.5, inherit.aes = FALSE) +
  geom_rect(data = time_periods, aes(xmin = end), xmax = Inf, ymin = -Inf, ymax = Inf, fill = "black", alpha = 0.5, inherit.aes = FALSE) +
  geom_vline(data = time_periods, aes(xintercept = as.double(intv)))

ggsave("all deaths by site.jpg", all_deaths_plot, path = "plots/data quality/", width = 30, height = 20, units = "cm")
ggsave("all sec deaths by site.jpg", sec_deaths_plot, path = "plots/data quality/", width = 30, height = 20, units = "cm")

# val_names <- c("all", "any", sort(unique(data[sub_measure != "any" & sub_measure != "other", sub_measure])), "other")
# col_vals <- c("#000000",
#   "#9D9D9D",
#   "#FFFFFF",
#   "#BE2633",
#   "#E06F8B",
#   "#493C2B",
#   "#A46422",
#   "#EB8931",
#   "#F7E26B",
#   "#2F484E",
#   "#44891A",
#   "#A3CE27",
#   "#1B2632",
#   "#005784",
#   "#31A2F2",
#   "#B2DCEF")
# names(col_vals) <- val_names
  # scale_colour_manual(values = col_vals,
  #   breaks = c(sort(unique(data[sub_measure != "other", sub_measure])), "other"),
  #   name = guide_legend("Urgent Care Condition"))
#
#
# period_label <- c("period1_before", "period2_after")
# emergency_admissions_by_ucc_month[, period := period_label[as.integer(relative_month > 24) + 1]]
# emergency_admissions_by_sec_month[, period := period_label[as.integer(relative_month > 24) + 1]]
#
# # Collapse to before/after
# emergency_admissions_by_ucc_period <- emergency_admissions_by_ucc_month[, .(value = sum(value, na.rm = TRUE)), by = .(town, period, sub_measure)]
# emergency_admissions_by_sec_period <- emergency_admissions_by_sec_month[, .(value = sum(value, na.rm = TRUE)), by = .(town, period, sub_measure)]
# emergency_admissions_by_period <- emergency_admissions_by_ucc_period[, .(value = sum(value, na.rm = TRUE)), by = .(town, period)]
#
#
# emergency_admissions_by_ucc_period_wide <- dcast(emergency_admissions_by_ucc_period, sub_measure ~ town + period, value.var = "value")
# emergency_admissions_by_sec_period_wide <- dcast(emergency_admissions_by_sec_period, sub_measure ~ town + period, value.var = "value")
#
# write.table(emergency_admissions_by_ucc_period_wide, file = "clipboard", sep = "\t", row.names = FALSE)
# write.table(emergency_admissions_by_sec_period_wide, file = "clipboard", sep = "\t", row.names = FALSE)

