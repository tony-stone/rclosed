save_ed_attendances_measure <- function() {

  db_conn <- connect2DB()

  tbl_name <- "attendances_by_mode_trust_lsoa_month"
  add_logic <- ""
  add_fields <- "aearrivalmode"

  # Prepare query string to create temp table
  sql_create_tbl <- getSqlUpdateQuery("ae", tbl_name, add_logic, add_fields)

  # Takes ~1.5mins
  pc <- proc.time()
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_tbl)
  proc.time() - pc

  # retrieve data
  ed_attendances_by_mode_trust_lsoa_month <- getDataFromTempTable(db_conn, tbl_name, "ae", add_fields)

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  # save data
  save(ed_attendances_by_mode_trust_lsoa_month, file = "data/ed attendances by mode trust aetype lsoa month.Rda", compress = "xz")

  # collapse to attendances by (lsoa, month, mode of arrival)
  ed_attendances_by_mode_lsoa_month <- ed_attendances_by_mode_trust_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth, aearrivalmode)]

  # any mode of arrival
  attendances_by_lsoa_month <- ed_attendances_by_mode_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth)]
  attendances_by_lsoa_month[, aearrivalmode := "any"]

  # sub-categories of mode of arrival
  ed_attendances_by_mode_lsoa_month[aearrivalmode == "1", aearrivalmode := "ambulance"]
  ed_attendances_by_mode_lsoa_month[aearrivalmode == "2", aearrivalmode := "other"]

  # combine
  ed_attendances_by_mode_lsoa_month <- rbind(attendances_by_lsoa_month, ed_attendances_by_mode_lsoa_month)

  # remove "unknown" mode of arrival
  ed_attendances_by_mode_lsoa_month <- ed_attendances_by_mode_lsoa_month[aearrivalmode == "ambulance" | aearrivalmode == "other" | aearrivalmode == "any"]

  # format
  data.table::setnames(ed_attendances_by_mode_lsoa_month, "aearrivalmode", "sub_measure")
  ed_attendances_by_mode_lsoa_month[, measure := "ed attendances"]
  ed_attendances_by_mode_measure <- fillDataPoints(ed_attendances_by_mode_lsoa_month)

  save(ed_attendances_by_mode_measure, file = "data/ed attendances by mode measure.Rda", compress = "bzip2")
}






save_unnecessary_ed_attendances_measure <- function() {

  db_conn <- connect2DB()

  tbl_name <- "attendances_without_treatment"
  field_indexes <- c(paste0("0", 1:9), 10:12)
  add_logic <- paste("(aeattenddisp = '02' OR aeattenddisp = '03' OR aeattenddisp = '12') AND",
    paste0("(invest2_", field_indexes, " IS NULL OR substring(invest2_", field_indexes, " from 1 for 2) IN ('06', '6', '21', '22', '23', '24'))", collapse = " AND "), "AND",
    paste0("(treat2_", field_indexes, " IS NULL OR treat2_", field_indexes, " IN ('0', '00', '01', '1', '02', '2', '07', '7', '22', '30', '34', '56', '57', '99'))", collapse = " AND "), sep = " ")
  add_fields <- ""

  # Prepare query string to create temp table
  sql_create_tbl <- getSqlUpdateQuery("ae", tbl_name, add_logic, add_fields)

  # Takes ~25s
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_tbl)

  # retrieve data
  unnecessary_ed_attendances_by_trust_lsoa_month <- getDataFromTempTable(db_conn, tbl_name, "ae", "")

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  # save data
  save(unnecessary_ed_attendances_by_trust_lsoa_month, file = "data/unnecessary ed attendances by trust lsoa month.Rda", compress = "xz")

  # Colapse by (lsoa, month)
  unnecessary_ed_attendances_by_lsoa_month <- unnecessary_ed_attendances_by_trust_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth)]

  # format
  unnecessary_ed_attendances_by_lsoa_month[, ':=' (measure = "unnecessary ed attendances",
    sub_measure = as.character(NA))]
  unnecessary_ed_attendances_measure <- fillDataPoints(unnecessary_ed_attendances_by_lsoa_month)

  # save measure
  save(unnecessary_ed_attendances_measure, file = "data/unnecessary ed attendances measure.Rda", compress = "xz")
}



save_ed_attendances_admitted_measure <- function() {

  db_conn <- connect2DB()

  tbl_name <- "attendances_admitted"
  add_logic <- ""
  add_fields <- "aeattenddisp"

  # Prepare query string to create temp table
  sql_create_tbl <- getSqlUpdateQuery("ae", tbl_name, add_logic, add_fields)

  # Takes ~3.5mins
  resource <- RJDBC::dbSendUpdate(db_conn, sql_create_tbl)

  # retrieve data
  ed_attendances_by_disposal_trust_lsoa_month <- getDataFromTempTable(db_conn, tbl_name, "ae", "aeattenddisp")

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  # save data
  save(ed_attendances_by_disposal_trust_lsoa_month, file = "data/ed attendances by disposal trust lsoa month.Rda", compress = "xz")

  # Collapse by (lsoa, month, aeattenddisp)
  ed_attendances_by_disposal_lsoa_month <- ed_attendances_by_disposal_trust_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth, aeattenddisp)]
  data.table::setnames(ed_attendances_by_disposal_lsoa_month, "aeattenddisp", "sub_measure")

  # All attendances
  ed_attendances_by_lsoa_month <- ed_attendances_by_disposal_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth)]
  ed_attendances_by_lsoa_month[, sub_measure := "all"]

  # Attendances that are admitted
  ed_attendances_admitted_by_lsoa_month <- ed_attendances_by_disposal_lsoa_month[sub_measure == "01"]
  ed_attendances_admitted_by_lsoa_month[, sub_measure := "admitted"]

  # Fraction of all attendances admitted
  ed_attendances_admitted_frac_by_lsoa_month <- merge(ed_attendances_by_lsoa_month, ed_attendances_admitted_by_lsoa_month, by = c("lsoa", "yearmonth"), all.x = TRUE)
  ed_attendances_admitted_frac_by_lsoa_month[, ':=' (value = value.y / value.x,
    sub_measure.x = NULL,
    sub_measure.y = NULL,
    value.x = NULL,
    value.y = NULL,
    sub_measure = "fraction admitted")]

  # Combine
  ed_attendances_admitted_and_frac_by_lsoa_month <- rbind(ed_attendances_admitted_by_lsoa_month, ed_attendances_admitted_frac_by_lsoa_month, ed_attendances_by_lsoa_month)
  ed_attendances_admitted_and_frac_by_lsoa_month[, measure := "ed attendances admitted"]

  ed_attendances_admitted_measure <- fillDataPoints(ed_attendances_admitted_and_frac_by_lsoa_month)

  # save measure
  save(ed_attendances_admitted_measure, file = "data/ed attendances admitted measure.Rda", compress = "xz")
}



# bla <- ed_attendances_admitted_measure[sub_measure != "fraction admitted", .(value = sum(value)), by = .(sub_measure, town, yearmonth)]
#
# bla2 <- data.table::dcast(bla, town + yearmonth ~ sub_measure, value.var = "value")
#
# bla2[, value := admitted / all]
#
# ggplot2::ggplot(bla2[town %in% c("Basingstoke", "Newark")], ggplot2::aes(x = yearmonth, y = value, colour = town)) +
#   #ggplot2::facet_grid(. ~ aedepttype_int) +
#   ggplot2::ggtitle("Unnecessary ED attendances by month\n(split by ED site)") +
#   ggplot2::geom_line(size = 1) +
#   ggplot2::scale_x_date(name = "month", date_breaks = "3 months", date_labels = "%b %Y", expand = c(0, 15)) +
#   ggplot2::scale_y_continuous(labels = scales::comma, limits = c(0, NA), expand = c(0.02, 0)) +
#   ggplot2::theme(axis.text.x = ggplot2::element_text(face="bold", angle=90, hjust=0.0, vjust=0.3))


save_ed_attendances_measure()
save_unnecessary_ed_attendances_measure()
save_ed_attendances_admitted_measure()
