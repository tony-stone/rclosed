library(data.table)

# Trust names (current and past) data from HSCIC

# Old data
trust_name_archived_data <- fread("data-raw/HSCIC provider codes/earchive 29May2015.csv", sep = ",", colClasses = "character",  header = FALSE)
trust_name_archived_data[, paste0("V", c(3:10, 13:27)) := NULL]
setnames(trust_name_archived_data, c("trust_code", "trust_name", "open_date", "close_date"))

# Keep only 3 character codes
trust_name_archived_data <- trust_name_archived_data[nchar(trust_code) == 3, ]
# Check trust codes unique
stopifnot(length(unique(trust_name_archived_data$trust_code)) == nrow(trust_name_archived_data))


trust_name_current_data <- fread("data-raw/HSCIC provider codes/etr 29May2015.csv", sep = ",", colClasses = "character",  header = FALSE)
trust_name_current_data[, paste0("V", c(3:10, 13:27)) := NULL]
setnames(trust_name_current_data, c("trust_code", "trust_name", "open_date", "close_date"))
# Check trust codes unique
stopifnot(length(unique(trust_name_current_data$trust_code)) == nrow(trust_name_current_data))

# Bind together archive and current data
trust_names_data <- rbind(trust_name_current_data, trust_name_archived_data)

# Check trust codes unique across archive and current data
stopifnot(length(unique(trust_names_data$trust_code)) == nrow(trust_name_data))

# Sort date types
trust_names_data[, ':=' (open_date = as.Date(open_date, format="%Y%m%d"),
  close_date = as.Date(close_date, format="%Y%m%d"))]

# save
save(trust_names_data, file = "data/trust names data.Rda")
