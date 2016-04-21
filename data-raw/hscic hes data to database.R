# Script uploads HES data (both APC and A&E) into database

# Connection to DB requires a 64bit download of Java (if running 64bit R) from https://www.java.com/en/download/manual.jsp
# Requires JDBC driver from https://jdbc.postgresql.org/download.html

# Load the database connection settings from rda file.
# The file must contain the list "db_config" with the following named items: host; port; db_name; user; pass.
load(file = "data/DB settings write.rda")

# Form the URL from the above set of params; ssl=true means we connect over SSL/TLS; Java, by default, checks server TLS/SSL certificate is valid
db_url <- paste0("jdbc:postgresql://", db_config["host"], ":", db_config["port"], "/", db_config["db_name"], "?user=", db_config["user"])

# Specify the driver, the Java driver (JAR) file, that Postgres uses double quotes (") to escape identifiers (keywords)
db_drvr <- RJDBC::JDBC("org.postgresql.Driver", "C:/Program Files/PostgreSQL/postgresql-9.4.1207.jar", "\"")

# Set up our DB connection
db_conn <- RJDBC::dbConnect(db_drvr, db_url, password = db_config["pass"])


# Upload AE data ----------------------------------------------------------

# Have a look at data
#test1 <- data.table::fread("D:/Rpackages/rclosed/data-raw/HSCIC HES data/Extract_Sheffield_AE_0708.txt", sep = "|", header = TRUE, na.strings = "", colClasses = "character", nrows = 10L)

#NOTE:
# invest2_nn is dirty (>2 chars)
# "aekey" is actually 12 characters (not 8 as specified), it is unique across reporting years (suspect first four chars are some play on the reporting year).

# Create AE table
sql_create_AE_table <- paste0("CREATE TABLE public.hes_ae_0714_raw (",
  "activage VARCHAR(3), arrivalage VARCHAR(4), ethnos VARCHAR(2), ",
  "postdist VARCHAR(4), encrypted_hesid VARCHAR(32), sex CHAR(1), ",
  "aearrivalmode CHAR(1), aeattendcat CHAR(1), aeattenddisp VARCHAR(2), ",
  "aedepttype VARCHAR(2), initdur VARCHAR(4), tretdur VARCHAR(4), ",
  "concldur VARCHAR(4), depdur VARCHAR(4), aeincloctype VARCHAR(2), ",
  "aepatgroup VARCHAR(2), aerefsource VARCHAR(2), arrivaldate VARCHAR(10), ",
  "arrivaltime VARCHAR(4), inittime VARCHAR(4), trettime VARCHAR(4), ",
  "concltime VARCHAR(4), deptime VARCHAR(4), diag_01 VARCHAR(6), ",
  "diag_02 VARCHAR(6), diag_03 VARCHAR(6), diag_04 VARCHAR(6), ",
  "diag_05 VARCHAR(6), diag_06 VARCHAR(6), diag_07 VARCHAR(6), ",
  "diag_08 VARCHAR(6), diag_09 VARCHAR(6), diag_10 VARCHAR(6), ",
  "diag_11 VARCHAR(6), diag_12 VARCHAR(6), diag2_01 VARCHAR(2), ",
  "diag2_02 VARCHAR(2), diag2_03 VARCHAR(2), diag2_04 VARCHAR(2), ",
  "diag2_05 VARCHAR(2), diag2_06 VARCHAR(2), diag2_07 VARCHAR(2), ",
  "diag2_08 VARCHAR(2), diag2_09 VARCHAR(2), diag2_10 VARCHAR(2), ",
  "diag2_11 VARCHAR(2), diag2_12 VARCHAR(2), invest_01 VARCHAR(6), ",
  "invest_02 VARCHAR(6), invest_03 VARCHAR(6), invest_04 VARCHAR(6), ",
  "invest_05 VARCHAR(6), invest_06 VARCHAR(6), invest_07 VARCHAR(6), ",
  "invest_08 VARCHAR(6), invest_09 VARCHAR(6), invest_10 VARCHAR(6), ",
  "invest_11 VARCHAR(6), invest_12 VARCHAR(6), invest2_01 VARCHAR(10), ",
  "invest2_02 VARCHAR(10), invest2_03 VARCHAR(10), invest2_04 VARCHAR(10), ",
  "invest2_05 VARCHAR(10), invest2_06 VARCHAR(10), invest2_07 VARCHAR(10), ",
  "invest2_08 VARCHAR(10), invest2_09 VARCHAR(10), invest2_10 VARCHAR(10), ",
  "invest2_11 VARCHAR(10), invest2_12 VARCHAR(10), treat_01 VARCHAR(6), ",
  "treat_02 VARCHAR(6), treat_03 VARCHAR(6), treat_04 VARCHAR(6), ",
  "treat_05 VARCHAR(6), treat_06 VARCHAR(6), treat_07 VARCHAR(6), ",
  "treat_08 VARCHAR(6), treat_09 VARCHAR(6), treat_10 VARCHAR(6), ",
  "treat_11 VARCHAR(6), treat_12 VARCHAR(6), treat2_01 VARCHAR(2), ",
  "treat2_02 VARCHAR(2), treat2_03 VARCHAR(2), treat2_04 VARCHAR(2), ",
  "treat2_05 VARCHAR(2), treat2_06 VARCHAR(2), treat2_07 VARCHAR(2), ",
  "treat2_08 VARCHAR(2), treat2_09 VARCHAR(2), treat2_10 VARCHAR(2), ",
  "treat2_11 VARCHAR(2), treat2_12 VARCHAR(2), oacode6 VARCHAR(6), ",
  "respct06 VARCHAR(3), resladst VARCHAR(4), imd04rk VARCHAR(5), ",
  "LSOA01 VARCHAR(9), MSOA01 VARCHAR(9), rururb_ind CHAR(1), ",
  "pcttreat VARCHAR(3), procode3 VARCHAR(3), procode VARCHAR(5), ",
  "procodet VARCHAR(5), aekey VARCHAR(12));")

# Ensure table does not already exist
stopifnot(RJDBC::dbExistsTable(db_conn, "hes_ae_0714_raw") == FALSE)

# Attempt to create table
AE_table_created <- tryCatch({
  RJDBC::dbSendUpdate(db_conn, sql_create_AE_table)
  TRUE
}, error = function(e)
  return(FALSE))
stopifnot(AE_table_created == TRUE)

# Upload data to table (returns FALSE for statements which failed to execute; TRUE for those that did)
yrs <- c(paste0("0", 7:9), 10:14)
sql_upload_data <- paste0("COPY public.hes_ae_0714_raw FROM '", getwd(), "/data-raw/HSCIC HES data/Extract_Sheffield_AE_", yrs[1:7], yrs[2:8], ".txt' (FORMAT CSV, DELIMITER '|', HEADER TRUE)")
data_uploaded <- lapply(sql_upload_data, function(sql_st) {
  tryCatch({
    RJDBC::dbSendUpdate(db_conn, sql_st)
    return("OK")
  }, error = function(e)
    return(paste0("FAILED: ", e)))
})

# Results of upload
data_uploaded

# Add unique constraint on "aekey" field
AE_aekey_unique <- tryCatch({
  RJDBC::dbSendUpdate(db_conn, "ALTER TABLE public.hes_ae_0714_raw ADD UNIQUE (aekey);")
  TRUE
}, error = function(e)
  return(FALSE))
stopifnot(AE_aekey_unique == TRUE)

# Total rows in table
RJDBC::dbGetQuery(db_conn, "SELECT COUNT(*) FROM public.hes_ae_0714_raw")


# Upload APC data ---------------------------------------------------------

# Have a look at data
#test2 <- data.table::fread("D:/Rpackages/rclosed/data-raw/HSCIC HES data/Extract_Sheffield_APC_1112.txt", sep = "|", header = TRUE, na.strings = "", colClasses = "character", nrows = 10L)

#NOTE:
# "epikey" is actually 12 characters (not 8 as specified), it is unique across reporting years (suspect first four chars are some play on the reporting year).
# "ethnos" from 2013/14 has max length of 2 chars (rather than 1)
#  For Year 2011/12, HSCIC included two extra fields (admi_cfl and dis_cfl, check flags for admidate and disdate)

# Creat APC table
sql_create_APC_table <- paste0("CREATE TABLE public.hes_apc_0714_raw (",
  "endage VARCHAR(4), startage VARCHAR(4), ethnos VARCHAR(2), ",
  "encrypted_hesid VARCHAR(32), postdist VARCHAR(4), sex CHAR(1), ",
  "admidate VARCHAR(10), admimeth VARCHAR(2), admisorc VARCHAR(2), ",
  "disdate VARCHAR(10), disdest VARCHAR(2), dismeth CHAR(1), ",
  "spelbgin CHAR(1), epiend VARCHAR(10), epistart VARCHAR(10), ",
  "speldur VARCHAR(5), spelend CHAR(1), epidur VARCHAR(5), ",
  "epiorder VARCHAR(2), disreadydate VARCHAR(10), diag_01 VARCHAR(6), ",
  "diag_02 VARCHAR(6), diag_03 VARCHAR(6), diag_04 VARCHAR(6), diag_05 VARCHAR(6), ",
  "diag_06 VARCHAR(6), diag_07 VARCHAR(6), diag_08 VARCHAR(6), diag_09 VARCHAR(6), ",
  "diag_10 VARCHAR(6), diag_11 VARCHAR(6), diag_12 VARCHAR(6), diag_13 VARCHAR(6), ",
  "diag_14 VARCHAR(6), diag_15 VARCHAR(6), diag_16 VARCHAR(6), diag_17 VARCHAR(6), ",
  "diag_18 VARCHAR(6), diag_19 VARCHAR(6), diag_20 VARCHAR(6), diag3_01 VARCHAR(3), ",
  "diag4_01 VARCHAR(4), cause VARCHAR(6), cause4 VARCHAR(4), cause3 VARCHAR(3), ",
  "intmanig CHAR(1), mainspef VARCHAR(3), tretspef VARCHAR(3), ",
  "procode VARCHAR(5), procode3 VARCHAR(3), procodet VARCHAR(5), ",
  "oacode6 VARCHAR(6), resladst VARCHAR(4), respct06 VARCHAR(3), LSOA01 VARCHAR(9), ",
  "MSOA01 VARCHAR(9), rururb_ind CHAR(1), imd04rk VARCHAR(5), ",
  "epikey VARCHAR(12));")

# Ensure table does not already exist
stopifnot(RJDBC::dbExistsTable(db_conn, "hes_apc_0714_raw") == FALSE)

# Attempt to create table
APC_table_created <- tryCatch({
  RJDBC::dbSendUpdate(db_conn, sql_create_APC_table)
  TRUE
  }, error = function(e)
    return(FALSE))
stopifnot(APC_table_created == TRUE)

# Upload data to table (returns error msgs for statements which fail to execute; "OK" for those that do execute successfully)
yrs <- c(paste0("0", 7:9), 10:14)
sql_upload_data <- paste0("COPY public.hes_apc_0714_raw FROM '", getwd(), "/data-raw/HSCIC HES data/Extract_Sheffield_APC_", yrs[1:7], yrs[2:8], ".txt' (FORMAT CSV, DELIMITER '|', HEADER TRUE)")
data_uploaded <- lapply(sql_upload_data, function(sql_st) {
  tryCatch({
    RJDBC::dbSendUpdate(db_conn, sql_st)
    return("OK")
  }, error = function(e)
    return(paste0("FAILED: ", e)))
  })

# Results
data_uploaded


# Upload 2011/12 APC data -------------------------------------------------

# Read data into R
HES_APC_1112_data <- fread("data-raw/HSCIC HES data/Extract_Sheffield_APC_1112.txt", sep = "|", header = TRUE, colClasses = "character")

# Drop HSCIC's additional fields
HES_APC_1112_data[, c("adm_cfl", "dis_cfl") := NULL]

# Write out editted file (faster than using dbWriteTable!)
write.csv(HES_APC_1112_data, file = "data-raw/HSCIC HES data/Extract_Sheffield_APC_1112_editted.csv", row.names = FALSE)

# Free up memeory
rm(HES_APC_1112_data)
gc()

# Upload
data_uploaded <- tryCatch({
  RJDBC::dbSendUpdate(db_conn, paste0("COPY public.hes_apc_0714_raw FROM '", getwd(), "/data-raw/HSCIC HES data/Extract_Sheffield_APC_1112_editted.csv' (FORMAT CSV, HEADER TRUE)"))
  TRUE
}, error = function(e)
  return(paste0("FAILED: ", e)))
data_uploaded


# Tidy up database table --------------------------------------------------

# Add unique constraint on "epikey" field
APC_epikey_unique <- tryCatch({
  RJDBC::dbSendUpdate(db_conn, "ALTER TABLE public.hes_apc_0714_raw ADD UNIQUE (epikey)")
  TRUE
}, error = function(e)
  return(FALSE))
stopifnot(APC_epikey_unique == TRUE)

# Total rows in table
RJDBC::dbGetQuery(db_conn, "SELECT COUNT(*) FROM public.hes_apc_0714_raw")
