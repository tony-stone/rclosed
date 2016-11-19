library(data.table)

# Read data into R
HES_linked_mortality <- fread("data-raw/HSCIC HES linked mortality/extract_NIC392342_ONS_2_APPROVED_4317_05102016_13.txt", sep = "|", header = TRUE, colClasses = "character")
setnames(HES_linked_mortality, c("encrypted_hesid", "cause.code.0",
  "pct_of_residence", "sex", "match_rank", "death_record_used",
  "communal_establishment", "nhs_indicator", "date_of_death",
  paste0("cause.code.", 1:15), "lsoa", "startage"))

HES_linked_mortality[, startage := as.integer(startage)]

# ensure no one dies more than once
stopifnot(HES_linked_mortality[, .N, by = encrypted_hesid][N > 1, .N] == 0)

# ensure we have the same number of records as NHS Digital declared
stopifnot(HES_linked_mortality[, .N] == 1631151)

HES_linked_mortality[, c("pct_of_residence", "match_rank", "death_record_used",
  "communal_establishment", "nhs_indicator") := NULL]

save(HES_linked_mortality, file = "data/hes linked mortality.Rda", compress = "xz")
