library(ggplot2)

# Creates new DB table to store only relevant HES AE records based on supplied LSOAs, aeattendcat, aedepttype
get_ed_attendances_data <- function() {

  load(file = "data/DB settings read.rda")

  # Form the URL from the above set of params; ssl=true means we connect over SSL/TLS; Java, by default, checks server TLS/SSL certificate is valid
  db_url <- paste0("jdbc:postgresql://", db_config["host"], ":", db_config["port"], "/", db_config["db_name"], "?user=", db_config["user"], "&defaultAutoCommit")

  # Specify the driver, the Java driver (JAR) file, and that Postgres uses double quotes (") to escape identifiers (keywords)
  db_drvr <- RJDBC::JDBC("org.postgresql.Driver", "C:/Program Files/PostgreSQL/postgresql-9.4.1207.jar", "\"")

  # Set up our DB connection
  db_conn <- DBI::dbConnect(db_drvr, db_url, password = db_config["pass"])


  # Prepare query string to create temp table
  sql_query_temp_table <- paste("CREATE TEMP TABLE admissions_by_mode_trust_lsoa_month AS",
    "SELECT row_number() OVER () AS row, procode3, lsoa01, aedepttype, yearmonth, aearrivalmode, value",
    "FROM (",
    "SELECT procode3, lsoa01, aedepttype, substring(arrivaldate from 1 for 7) AS yearmonth, aearrivalmode, COUNT(*) AS value FROM relevant_ae_attendances",
    "wHERE aeattendcat = '1' AND (aedepttype = '01' OR aedepttype = '99')",
    "GROUP BY procode3, lsoa01, aedepttype, substring(arrivaldate from 1 for 7), aearrivalmode",
    ") AS st;", sep = " ")

  # Takes about 2.5mins
  resource <- RJDBC::dbSendUpdate(db_conn, sql_query_temp_table)

  # Get size of temp table
  nrows <- DBI::dbGetQuery(db_conn, "SELECT COUNT(*) FROM admissions_by_mode_trust_lsoa_month;")[1, 1]

  # Set offset and limit var
  offset <- 0L
  limit <- 500000L
  sql_select <- "SELECT procode3, lsoa01, aedepttype, yearmonth, aearrivalmode, value FROM admissions_by_mode_trust_lsoa_month "

  # Entire data retrieval takes ~13s
#  pc <- proc.time()

  sql_query_select <- paste0(sql_select, "WHERE row > ", offset, " AND row <= ", offset + limit, ";")
  attendances_by_mode_trust_lsoa_month <- DBI::dbGetQuery(db_conn, sql_query_select)

  # Need to call gc() to clear up Java heap space
  gc()
  # Increment offset
  offset <- offset + limit

  # Loop to fetch all rows of data (takes ~45s in entirety)
  while (offset < nrows) {
    sql_query_select <- paste0(sql_select, "WHERE row > ", offset, " AND row <= ", offset + limit, ";")
    attendances_by_mode_trust_lsoa_month <- rbind(attendances_by_mode_trust_lsoa_month, DBI::dbGetQuery(db_conn, sql_query_select))
    gc()
    offset <- offset + limit
  }

#  print(proc.time() - pc)

  # Disconnect from DB
  DBI::dbDisconnect(db_conn)
  db_conn <- NULL

  # Convert to data.table
  attendances_by_mode_trust_lsoa_month <- data.table::data.table(attendances_by_mode_trust_lsoa_month)

  # Convert dates to date type
  attendances_by_mode_trust_lsoa_month[, yearmonth := as.Date(lubridate::fast_strptime(paste0(yearmonth, "-01"), format = "%Y-%m-%d"))]

  # Standardise field names
  data.table::setnames(attendances_by_mode_trust_lsoa_month, "lsoa01", "lsoa")

  # save data
  save(attendances_by_mode_trust_lsoa_month, file = "data/attendances by mode trust aetype lsoa month.Rda", compress = "xz")

  attendances_by_mode_lsoa_month <- attendances_by_mode_trust_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth, aearrivalmode)]

  #sapply(attendances_by_mode_lsoa_month, function(field) if(length(unique(field)) > 100) { ">100 distinct values" } else { sort(unique(field), na.last = TRUE) })

  attendances_by_lsoa_month <- attendances_by_mode_lsoa_month[, .(value = sum(value)), by = .(lsoa, yearmonth)]
  attendances_by_lsoa_month[, aearrivalmode := "any"]

  attendances_by_mode_lsoa_month[aearrivalmode == "1", aearrivalmode := "ambulance"]
  attendances_by_mode_lsoa_month[aearrivalmode == "2", aearrivalmode := "other"]

  attendances_by_mode_lsoa_month <- rbind(attendances_by_lsoa_month, attendances_by_mode_lsoa_month)
  attendances_by_mode_measure <- attendances_by_mode_lsoa_month[aearrivalmode == "ambulance" | aearrivalmode == "other" | aearrivalmode == "any"]
  data.table::setnames(attendances_by_mode_measure, "aearrivalmode", "sub_measure")
  attendances_by_mode_measure[, measure := "ed attendances"]

  # Create datapoints for each month/lsoa/measure/submeasure covering the measurement space (as this is not guaranteed from the data)
  # Merge in catchment data
  load("data/catchment area set final.Rda")
  data_points <- data.table::data.table(expand.grid(lsoa = unique(catchment_area_set_final$lsoa), yearmonth = unique(attendances_by_mode_measure$yearmonth), measure = unique(attendances_by_mode_measure$measure), sub_measure = unique(attendances_by_mode_measure$sub_measure), stringsAsFactors = FALSE))

  # Merge data so we have a data for each and every point (set to 0 where applicable)
  attendances_by_mode_measure <- merge(data_points, attendances_by_mode_measure, by = c("lsoa", "yearmonth", "measure", "sub_measure"), all = TRUE)
  attendances_by_mode_measure[is.na(value), value := 0]

  # Merge in catchment area data
  ed_attendances_by_mode_measure <- merge(attendances_by_mode_measure, catchment_area_set_final, by = "lsoa", all = TRUE)

  ed_attendances_by_mode_measure[, time_to_ed := time_to_ae_post_intv]
  ed_attendances_by_mode_measure[intervention_date > yearmonth, time_to_ed := time_to_ae_pre_intv]
  ed_attendances_by_mode_measure[, c("time_to_ae_pre_intv", "time_to_ae_post_intv", "intervention_date") := NULL]

  save(ed_attendances_by_mode_measure, file = "data/ed attendances by mode measure.Rda", compress = "bzip2")
}
