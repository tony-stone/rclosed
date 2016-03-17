library(data.table)
library(ggplot2)
library(lubridate)

# Function to read in annual AE HES data and convert to annual attendances by procode/lsoa/month

save_HES_AE_attendances_data <- function() {

  load(file = "data/DB settings read.rda")

  # Form the URL from the above set of params; ssl=true means we connect over SSL/TLS; Java, by default, checks server TLS/SSL certificate is valid
  db_url <- paste0("jdbc:postgresql://", db_config["host"], ":", db_config["port"], "/", db_config["db_name"], "?user=", db_config["user"], "&defaultAutoCommit")

  # Specify the driver, the Java driver (JAR) file, and that Postgres uses double quotes (") to escape identifiers (keywords)
  db_drvr <- RJDBC::JDBC("org.postgresql.Driver", "C:/Program Files/PostgreSQL/postgresql-9.4.1207.jar", "\"")

  # Set up our DB connection
  db_conn <- DBI::dbConnect(db_drvr, db_url, password = db_config["pass"])

  # Prepare query string to create temp table
  sql_query_temp_table <- paste("CREATE TEMP TABLE admissions_by_trust_lsoa_month AS",
    "SELECT row_number() OVER () AS row, procode3, lsoa01, aedepttype, yearmonth, attendances",
    "FROM (",
    "SELECT procode3, lsoa01, aedepttype, substring(arrivaldate from 1 for 7) AS yearmonth, COUNT(*) AS attendances FROM public.hes_ae_0714_raw",
    "wHERE aeattendcat = '1' AND (aedepttype = '01' OR aedepttype = '99')",
    "GROUP BY procode3, lsoa01, aedepttype, substring(arrivaldate from 1 for 7)",
    ") AS st;", sep = " ")

  # Takes about 12mins
  resource <- RJDBC::dbSendUpdate(db_conn, sql_query_temp_table)

  # Get size of temp table
  nrows <- DBI::dbGetQuery(db_conn, "SELECT COUNT(*) FROM admissions_by_trust_lsoa_month;")[1, 1]

  # Set offset and limit var
  offset <- 0L
  limit <- 500000L

  # Query table - takes ~12mins
  sql_query_select <- paste0("SELECT procode3, lsoa01, aedepttype, yearmonth, attendances ",
    "FROM admissions_by_trust_lsoa_month ",
    "WHERE row > ", offset, " AND row <= ", offset + limit, ";")
  attendances_by_trust_lsoa_month <- DBI::dbGetQuery(db_conn, sql_query_select)

  # Need to call gc() to clear up Java heap space
  gc()
  # Increment offset
  offset <- offset + limit

  # Loop to fetch all rows of data (takes ~45s in entirety)
  while (offset < nrows) {
    sql_query_select <- paste0("SELECT procode3, lsoa01, aedepttype, yearmonth, attendances ",
      "FROM admissions_by_trust_lsoa_month ",
      "WHERE row > ", offset, " AND row <= ", offset + limit, ";")
    attendances_by_trust_lsoa_month <- rbind(attendances_by_trust_lsoa_month, DBI::dbGetQuery(db_conn, sql_query_select))
    gc()
    offset <- offset + limit
  }

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  # Convert to data.table
  attendances_by_trust_lsoa_month <- data.table::data.table(attendances_by_trust_lsoa_month)

  # save data
  save(attendances_by_trust_lsoa_month, file = "data/attendances by trust lsoa month.Rda", compress = "xz")
}




# ## Data quality checks
# # Completeness of aedepttype
# attendances_by_trust_month <- attendances_by_trust_lsoa_month[, .(attendances = sum(attendances)), by = .(procode3, yearmonth, aedepttype)]
# trusts <- attendances_by_trust_month[, .(first_appearance = min(yearmonth), last_appearance = max(yearmonth)), by = .(procode3, aedepttype)]
# load("data/site data.Rda")
#
# site_data_select <- unique(site_data[, .(trust_code, is_intervention)])
#
# plot.data <- attendances_by_trust_month[, .(attendances, yearmonth, procode3, aedepttype, trust_of_interest = (procode3 %in% site_data_select$trust_code))]
# plot.data[, yearmonth := as.Date(fast_strptime(paste0(yearmonth, "-01"), format = "%Y-%m-%d"))]
#
# plot.data1 <- plot.data[, .(attendances = sum(attendances)), by = .(yearmonth, trust_of_interest, aedepttype)]
#
#
# ggplot(plot.data1[, .(attendances = sum(attendances)), by = .(yearmonth, aedepttype)], aes(x = yearmonth, y = attendances, colour = aedepttype)) +
#   ggtitle("A&E attendances by month\n(split by AE department type)") +
#   geom_line(size = 1) +
#   scale_x_date(name = "month", date_breaks = "3 months", date_labels = "%b %Y", limits = c(as.Date("2007-04-01"), NA), expand = c(0, 15)) +
#   scale_y_continuous(labels = scales::comma) +
#   theme(axis.text.x = element_text(face="bold", angle=90, hjust=0.0, vjust=0.3))
#
# ggplot(plot.data1[trust_of_interest == TRUE], aes(x = yearmonth, y = attendances, colour = aedepttype)) +
#   ggtitle("A&E attendances by month for intervention/control NHS Trust \n(split by AE department type)") +
#   geom_line(size = 1) +
#   scale_x_date(name = "month", date_breaks = "3 months", date_labels = "%b %Y", limits = c(as.Date("2007-04-01"), NA), expand = c(0, 15)) +
#   scale_y_continuous(labels = scales::comma) +
#   theme(axis.text.x = element_text(face="bold", angle=90, hjust=0.0, vjust=0.3))
#
# plot.data2 <- rbind(plot.data1[, .(attendances = sum(attendances), only_trusts_of_interest = FALSE), by = .(yearmonth)], plot.data1[trust_of_interest == TRUE, .(attendances = sum(attendances), only_trusts_of_interest = TRUE), by = .(yearmonth)])
# ggplot(plot.data2, aes(x = yearmonth, y = attendances, colour = only_trusts_of_interest)) +
#   ggtitle("Total A&E attendances by month\n(all A&Es vs only intervention/control NHS Trusts)") +
#   geom_line(size = 1) +
#   scale_x_date(name = "month", date_breaks = "3 months", date_labels = "%b %Y", limits = c(as.Date("2007-04-01"), NA), expand = c(0, 15)) +
#   scale_y_continuous(labels = scales::comma) +
#   theme(axis.text.x = element_text(face="bold", angle=90, hjust=0.0, vjust=0.3))
#
# plot.data3 <- merge(plot.data[trust_of_interest == TRUE, ], site_data_select, by.x = "procode3", by.y = "trust_code")
# ggplot(plot.data3[, .(attendances = sum(attendances)), by = .(yearmonth, is_intervention)], aes(x = yearmonth, y = attendances, colour = is_intervention)) +
#   ggtitle("Total A&E attendances by month for intervention/control NHS Trusts only\n(split by intervention or control)") +
#   geom_line(size = 1) +
#   geom_vline(xintercept = as.integer(as.Date(paste0(c("2009-03", "2009-10", "2010-08", "2011-04", "2011-08"), "-01")))) +
#   scale_x_date(name = "month", date_breaks = "3 months", date_labels = "%b %Y", limits = c(as.Date("2007-04-01"), NA), expand = c(0, 15)) +
#   scale_y_continuous(labels = scales::comma) +
#   theme(axis.text.x = element_text(face="bold", angle=90, hjust=0.0, vjust=0.3))
