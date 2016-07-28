library(data.table)
library(openxlsx)


# Make site name lower case
clean_string <- function(x) {
  x <- tolower(x)
  # Replace dashes (-) with single space
  x <- gsub("-{1,}", " ", x, ignore.case=F, fixed=F)
  # Replace whitespace characters with single space
  x <- gsub("\\s{1,}", " ", x, ignore.case=F, fixed=F)
  # Remove non-alphabet characters (and non-spaces)
  x <- gsub("[^a-z ]", "", x, ignore.case=F, fixed=F)
  # Remove double (or more) spaces
  x <- gsub(" {1,}", " ", x, ignore.case=F, fixed=F)
  # Remove leading and trailing spaces
  x <- trimws(x)
}

clean_hospital_name <- function(x) {
  x <- clean_string(x)
  # Remove "the " from start of string
  x <- sub("^the ", "", x, ignore.case=F, fixed=F)
  # Remove "hospital(s)" from the end
  x <- sub(" hospitals?$", "", x, ignore.case=F, fixed=F)
}


clean_trust_name <- function(x) {
  x <- clean_string(x)
  # Remove "the " from start of string
  x <- sub("^the ", "", x, ignore.case=F, fixed=F)
  # Remove anything after " trust " from the end
  x <- sub(" trust [a-z0-9 ]+$", " trust", x, ignore.case=F, fixed=F)
}

clean_code <- function(x) {
  # Make postcode name upper case
  x <- toupper(x)
  # Replace whitespace characters with single space
  x <- gsub("\\s{1,}", " ", x, ignore.case=F, fixed=F)
  # Remove non-alphanumeric characters (and non-spaces)
  x <- gsub("[^A-Z0-9 ]", "", x, ignore.case=F, fixed=F)
  # Remove double (or more) spaces
  x <- gsub(" {1,}", " ", x, ignore.case=F, fixed=F)
  # Remove leading spaces and trailing spaces
  x <- trimws(x)
}


# read in data and set colnames
ed_sites_2012 <- fread("data-raw/ED sites data/2012 hospitals raw data.csv", sep = ",", header = TRUE, colClasses = "character", na.strings = c("NULL", "NA", ""))
ed_sites_2015 <- fread("data-raw/ED sites data/2015 hospitals raw data.csv", sep = ",", header = TRUE, colClasses = "character", na.strings = c("NULL", "NA", ""))

standard_cols <- c("site_code", "hospital_name", "trust_name", "address1", "address2", "address3", "address4", "address5", "postcode", "data_source")
setnames(ed_sites_2012, standard_cols)
setnames(ed_sites_2015, c(standard_cols, "comments"))

ed_sites_2015[, year := 2015L]
ed_sites_2012[, ':=' (comments = as.character(NA),
  year = 2012L)]

# Combine data sources
ed_sites_data <- rbind(ed_sites_2012, ed_sites_2015)


# Read in Scottish / Welsh data -------------------------------------------

ed_sites_scottish <- fread("data-raw/ED sites data/2015 Scottish A&E sites.csv", sep = ",", header = TRUE, colClasses = "character", na.strings = c("NULL", "NA", ""))
ed_sites_welsh <- fread("data-raw/ED sites data/2015 Welsh A&E sites.csv", sep = ",", header = TRUE, colClasses = "character", na.strings = c("NULL", "NA", ""))

standard_cols_sw <- c("site_name", "hospital_name", "trust_name", "postcode", "comments")
setnames(ed_sites_scottish, c("site.name", "hospital.name", "trust.name", "postcode", "comments"), standard_cols_sw)
setnames(ed_sites_welsh, c("site.name", "hospital.name", "trust.name", "postcode", "comments"), standard_cols_sw)
ed_sites_scottish[, colnames(ed_sites_scottish)[!(colnames(ed_sites_scottish) %in% standard_cols_sw)] := NULL, with = FALSE]
ed_sites_welsh[, colnames(ed_sites_welsh)[!(colnames(ed_sites_welsh) %in% standard_cols_sw)] := NULL, with = FALSE]

ed_sites_not_english <- rbind(ed_sites_scottish, ed_sites_welsh)
ed_sites_not_english[, ':=' (english = FALSE,
  open_date = as.Date(NA),
  closure_date = as.Date(NA),
  trust_code = as.character(NA))]


# Read in pre-2012 closures -----------------------------------------------

ed_sites_pre2012 <- data.table(read.xlsx("data-raw/site data/site data - 2016.05.18.xlsx"))[site_type == "intervention", .(site_name = clean_hospital_name(site_name), hospital_name = clean_hospital_name(site_name), trust_code, postcode = clean_code(postcode), closure_date = as.Date(intervention_date, origin = "1899-12-30"), english = TRUE)]


# clean data (site name, trust name and postcode) -------------------------

ed_sites_data[, c("site_code", "hospital_name", "trust_name", "postcode", paste0("address", 1:5)) := list( clean_code(site_code), clean_hospital_name(hospital_name), clean_trust_name(trust_name), clean_code(postcode), clean_string(address1), clean_string(address2), clean_string(address3), clean_string(address4), clean_string(address5) )]

# Some sources have duplicates - remove these (and any blanks)
ed_sites_data <- unique(ed_sites_data)[!is.na(hospital_name)]

# Create trust_code var
ed_sites_data[, trust_code := substring(site_code, 1, 3)]

# Fix specific site names for comparability -------------------------------

## Simple name changes/standardisations/corrections


ed_sites_data[hospital_name == "aintree university", hospital_name := "university hospital aintree"]
ed_sites_data[hospital_name == "alder hey childrens nhs foundation trust", hospital_name := "alder hey childrens"]
ed_sites_data[hospital_name == "alexandra hospital in redditch", hospital_name := "alexandra"]
ed_sites_data[hospital_name == "arrow park", hospital_name := "arrowe park"]
ed_sites_data[hospital_name == "bedford hospital south wing", hospital_name := "bedford"]
ed_sites_data[hospital_name == "broomfield mid essex", hospital_name := "broomfield"]
ed_sites_data[hospital_name == "county hospital wye valley nhs trust" |
    hospital_name == "county hospital wye valley nhs", hospital_name := "county"]
ed_sites_data[hospital_name == "diana", hospital_name := "diana princess of wales"]
ed_sites_data[hospital_name == "epsom", hospital_name := "epsom general"]
ed_sites_data[hospital_name == "friarage hospital site", hospital_name := "friarage"]
ed_sites_data[hospital_name == "george eliot hospital acute services", hospital_name := "george eliot"]
ed_sites_data[hospital_name == "harrogate", hospital_name := "harrogate district"]
ed_sites_data[hospital_name == "king college", hospital_name := "kings college"]
ed_sites_data[hospital_name == "kings college hospital denmark hill", hospital_name := "kings college"]
ed_sites_data[hospital_name == "lewisham", hospital_name := "university hospital lewisham"]
ed_sites_data[hospital_name == "lincoln county hospital incomplete data", hospital_name := "lincoln county"]
ed_sites_data[hospital_name == "luton and dunstable university", hospital_name := "luton and dunstable"]
ed_sites_data[hospital_name == "milton keynes university", hospital_name := "milton keynes"]
ed_sites_data[hospital_name == "norfolk and norwich university hospital nnuh", hospital_name := "norfolk and norwich university"]
ed_sites_data[hospital_name == "north middlesex university hospital nhs trust", hospital_name := "north middlesex university"]
ed_sites_data[hospital_name == "nottingham university hospitals nhs trust queens medical centre campus" |
    hospital_name == "nottingham university hospitals nhs trust queens medical centre", hospital_name := "queens medical centre"]
ed_sites_data[hospital_name == "queens medical center", hospital_name := "queens medical centre"]
ed_sites_data[hospital_name == "pool" |
    hospital_name == "poole", hospital_name := "poole general"]
ed_sites_data[hospital_name == "qe", hospital_name := "queen elizabeth"]
ed_sites_data[hospital_name == "queen elizabeth hospital birmingham", hospital_name := "queen elizabeth"]
ed_sites_data[hospital_name == "queen elizabeth hospital woolwich", hospital_name := "queen elizabeth"]
ed_sites_data[hospital_name == "queens hospital burton upon trent", hospital_name := "queens"]
ed_sites_data[hospital_name == "rotherham", hospital_name := "rotherham general"]
ed_sites_data[hospital_name == "royal bournemouth", hospital_name := "royal bournemouth general"]
ed_sites_data[hospital_name == "royal cornwall hospital treliske", hospital_name := "royal cornwall"]
ed_sites_data[hospital_name == "royal devon and exeter hospital wonford", hospital_name := "royal devon and exeter"]
ed_sites_data[hospital_name == "royal surrey", hospital_name := "royal surrey county"]
ed_sites_data[hospital_name == "sandwell", hospital_name := "sandwell general"]
ed_sites_data[hospital_name == "sandwell district general", hospital_name := "sandwell general"]
ed_sites_data[hospital_name == "scarborough", hospital_name := "scarborough general"]
ed_sites_data[hospital_name == "st georges hospital london" |
    hospital_name == "st georges hospital tooting", hospital_name := "st georges"]
ed_sites_data[hospital_name == "st jamess hosptial", hospital_name := "st jamess"]
ed_sites_data[hospital_name == "st marys hospital hq", hospital_name := "st marys"]
ed_sites_data[hospital_name == "stafford", hospital_name := "county"]
ed_sites_data[hospital_name == "tameside", hospital_name := "tameside general"]
ed_sites_data[hospital_name == "torbay district", hospital_name := "torbay"]
ed_sites_data[hospital_name == "tunbridge wells hospital at pembury", hospital_name := "tunbridge wells"]
ed_sites_data[hospital_name == "university", hospital_name := "university hospital coventry"]
ed_sites_data[hospital_name == "university college london", hospital_name := "university college"]
ed_sites_data[hospital_name == "university hospital of north staffordshire", hospital_name := "royal stoke university"]
ed_sites_data[hospital_name == "wansbeck", hospital_name := "wansbeck general"]
ed_sites_data[hospital_name == "william harvey hospital ashford", hospital_name := "william harvey"]
ed_sites_data[hospital_name == "wonford", hospital_name := "royal devon and exeter"]

## More complicated changes
# Remove (probably confusion when "Univ. Hosp of North Staffs" became "Royal Stoke" and "Stafford Hosp" became "County Hosp".
# Trust became "university hospitals of north midlands", this looks like it is the trust's HQ rather than an A&E site
ed_sites_data <- ed_sites_data[hospital_name != "university hospitals of north midlands"]

## Postcode corrections
ed_sites_data[hospital_name == "countess of chester" & postcode == "CH2 1HJ", postcode := "CH2 1UL"]
ed_sites_data[hospital_name == "county" & postcode == "HR1 2BN", postcode := "HR1 2ER"]
ed_sites_data[hospital_name == "royal derby" & postcode == "DE22 3LZ", postcode := "DE22 3NE"]
ed_sites_data[hospital_name == "royal london" & postcode == "E1 1BZ", postcode := "E1 1BB"]
ed_sites_data[hospital_name == "royal cornwall" & postcode == "TR1 3LQ", postcode := "TR1 3LJ"]
ed_sites_data[hospital_name == "manchester royal infirmary" & postcode == "M13 9WL", postcode := "M13 9NZ"] # This is the postcode of the A&E dept.
ed_sites_data[hospital_name == "northumbria specialist emergency care" & is.na(postcode), postcode := "NE23 6NZ"]


# Identify hospital_namnes which are not unique within one or more -------
#   data sources

# Get distinct hospital names
non_distinct_hospital_names <- ed_sites_data[, .N, by = .(hospital_name, data_source)][N > 1, hospital_name]

# attach postcodes to non-unique hospitals in the Abdulwahid 2015 data
ed_sites_data[data_source == "Abdulwahid 2015" & hospital_name == "county" & trust_name == "university hospitals of north midlands nhs trust", postcode := "ST16 3SA"]
ed_sites_data[data_source == "Abdulwahid 2015" & hospital_name == "county" & trust_name == "wye valley nhs trust", postcode := "HR1 2ER"]
ed_sites_data[data_source == "Abdulwahid 2015" & hospital_name == "princess royal" & trust_name == "brighton and sussex university hospitals nhs trust", postcode := "RH16 4EX"]
ed_sites_data[data_source == "Abdulwahid 2015" & hospital_name == "princess royal" & trust_name == "shrewsbury and telford hospital nhs trust", postcode := "TF1 6TF"]
ed_sites_data[data_source == "Abdulwahid 2015" & hospital_name == "queen elizabeth" & trust_name == "gateshead health nhs foundation trust", postcode := "NE9 6SX"]
ed_sites_data[data_source == "Abdulwahid 2015" & hospital_name == "queen elizabeth" & trust_name == "queen elizabeth hospital kings lynn nhs foundation trust", postcode := "PE30 4ET"]
ed_sites_data[data_source == "Abdulwahid 2015" & hospital_name == "queen elizabeth" & trust_name == "university hospitals birmingham nhs foundation trust", postcode := "B15 2WB"]
ed_sites_data[data_source == "Abdulwahid 2015" & hospital_name == "queens" & trust_name == "barking havering and redbridge", postcode := "RM7 0AG"]
ed_sites_data[data_source == "Abdulwahid 2015" & hospital_name == "queens" & trust_name == "burton hospitals nhs foundation trust", postcode := "DE13 0RB"]
ed_sites_data[data_source == "Abdulwahid 2015" & hospital_name == "st marys" & trust_name == "isle of wight nhs trust", postcode := "PO30 5TG"]

# ensure all non-distinct hospital names have a postcode
stopifnot(all(!is.na(ed_sites_data[hospital_name %in% non_distinct_hospital_names, postcode])))

# Create unique name for each site -----------------------------------
# For most this will just be the hospital name
ed_sites_data[, site_name := hospital_name]
# But for those with ambigous hospital names, concatenate the postcode
ed_sites_data[hospital_name %in% non_distinct_hospital_names,  site_name := paste0(hospital_name, " (", postcode, ")")]


# Check we now have unique names for each site ----------------------------
stopifnot(all(ed_sites_data[, .N, by = .(site_name, data_source)][, N] == 1))
# ... and check only 1 sitename per postcode (exception for co-located "royal alexandra childrens" & "royal sussex county")
stopifnot(all(ed_sites_data[postcode != "BN2 5BE", .(distinct_sites = length(unique(site_name))), by = postcode][, distinct_sites] == 1) & ed_sites_data[postcode == "BN2 5BE", .(distinct_sites = length(unique(site_name))), by = postcode][, distinct_sites] == 2)


## Trust code corrections
ed_sites_data[site_name == "barnet", trust_code := "RAL"]
ed_sites_data[site_name == "county (ST16 3SA)", trust_code := "RJE"]
ed_sites_data[site_name == "ealing", trust_code := "R1K"]
ed_sites_data[site_name == "northumbria specialist emergency care", trust_code := "RTF"]
ed_sites_data[site_name == "northwick park", trust_code := "R1K"]
ed_sites_data[site_name == "princess royal university", trust_code := "RJZ"]
ed_sites_data[site_name == "queen elizabeth (SE18 4QH)", trust_code := "RJ2"]
ed_sites_data[site_name == "scarborough general", trust_code := "RCB"]
ed_sites_data[site_name == "st marys (PO30 5TG)", trust_code := "R1F"]
ed_sites_data[site_name == "st marys (TR21 0LE)", trust_code := "RJ8"]
ed_sites_data[site_name == "wexham park", trust_code := "RDU"]

# Check we only have exactly one trust_code (excld. NAs) per site_name
stopifnot(all(ed_sites_data[, .(distinct_tcodes = sum(!is.na(unique(trust_code)))), by = site_name][, distinct_tcodes] == 1))

# Check we only have exactly one postcode (excld. NAs) per site_name
stopifnot(all(ed_sites_data[, .(distinct_pcodes = sum(!is.na(unique(postcode)))), by = site_name][, distinct_pcodes] == 1))





# Create list of ED sites -------------------------------------------------

ed_sites <- unique(ed_sites_data[, .(site_name, hospital_name)])
ed_sites[, english := TRUE]

# Merge in trust and postcode data
ed_sites <- merge(ed_sites, unique(ed_sites_data[!is.na(trust_code), .(site_name, trust_code)]), by = "site_name", all.x = TRUE)
ed_sites <- merge(ed_sites, unique(ed_sites_data[!is.na(postcode), .(site_name, postcode)]), by = "site_name", all.x = TRUE)
stopifnot(all(!is.na(ed_sites[, trust_code])) & all(!is.na(ed_sites[, postcode])))

# Add A&E's which closed prior to 2012
stopifnot(!any(ed_sites_pre2012$site_name %in% ed_sites$site_name))
ed_sites <- rbind(ed_sites[, closure_date := as.Date(NA)], ed_sites_pre2012)

# Add and combine comments data
ed_sites <- merge(ed_sites, ed_sites_data[data_source == "Abdulwahid 2015" & !is.na(comments), .(site_name, abdulwahid_comments = comments)], by = "site_name", all.x = TRUE)

# Tony Stone comments
ed_sites[site_name %in% c("north tyneside general", "hexham general", "wansbeck general"), ':=' (comments = "A&E closed June 2015. North Tyneside and Northumberland re-structured their A&E provision. See also northumbria specialist emergency care.",
  closure_date = as.Date("2015-06-01"))]
ed_sites[site_name == "northumbria specialist emergency care", ':=' (comments = "A&E opened June 2015. North Tyneside and Northumberland re-structured their A&E provision. See also north tyneside general, hexham general, and wansbeck general.",
  open_date = as.Date("2015-06-01"))]
ed_sites[site_name == "trafford general", ':=' (comments = "Urgent Care Centre. Type 1 A&E until 28 Nov 2013.",
  closure_date = as.Date("2013-11-28"))]
ed_sites[site_name == "kent and canterbury", ':=' (comments = "Emergency Care Centre, no trauma. Not full Type 1 A&E (source: 2015 CQC report / BBC News.)",
  closure_date = as.Date("2005-01-01"))]
ed_sites[site_name == "solihull", ':=' (comments = "No ambulance-borne trauma care.  Not full Type 1 A&E (source: 2015 CQC report / UK Parliament publications.)",
  closure_date = as.Date("2002-01-01"))]
ed_sites[site_name == "county (ST16 3SA)", ':=' (comments = "Limited hours: 8am - 10pm.  Not full Type 1 A&E (by definition) (source: 'The Express & Star' website).",
  closure_date = as.Date("2011-12-01"))]
ed_sites[site_name == "pontefract", comments := "Some overnight closures in 2014."]
ed_sites[site_name == "central middlesex", ':=' (comments = "Urgent Care Centre. Type 1 A&E until 10 Sept 2014.",
  closure_date = as.Date("2014-09-10"))]

ed_sites[site_name == "hemel hempstead", comments := "A&E closed Mar 2009."]
ed_sites[site_name == "bishop auckland general", comments := "A&E closed Oct 2009."]
ed_sites[site_name == "newark", comments := "A&E closed Apr 2011."]
ed_sites[site_name == "rochdale infirmary", comments := "A&E closed Apr 2011."]
ed_sites[site_name == "university hospital of hartlepool", comments := "A&E closed Aug 2011."]

# combine
ed_sites[!is.na(abdulwahid_comments) & !is.na(comments), comments := paste0(comments, " (Abdulwahid 2015 comments: ", abdulwahid_comments, ")")]
ed_sites[!is.na(abdulwahid_comments) & is.na(comments), comments := paste0("(Abdulwahid 2015 comments: ", abdulwahid_comments, ")")]
ed_sites[, abdulwahid_comments := NULL]

# Add trust name
load("data/trust names data.Rda")
trust_names <- trust_names_data[, .(trust_code = clean_code(trust_code), trust_name = clean_string(trust_name))]
ed_sites <- merge(ed_sites, trust_names, by = "trust_code", all.x = TRUE)
stopifnot(all(!is.na(ed_sites[, trust_name])))

# Type 2 A&E?
ed_sites[, type2_ae := grepl("(^| )(eye|dental)($| )", hospital_name, perl = TRUE)]
ed_sites <- ed_sites[type2_ae == FALSE]
ed_sites[, type2_ae := NULL]


# Add non-english sites ---------------------------------------------------

# First check no name clashes
stopifnot(!any(ed_sites_not_english$site_name %in% ed_sites$site_name))
ed_sites <- rbind(ed_sites, ed_sites_not_english)


# Attach futher details ---------------------------------------------------

# How many of the 2015 data sources contain the sites
ed_sites <- merge(ed_sites, ed_sites_data[year == 2015, .(sources_2015 = .N), by = site_name], by = "site_name", all.x = TRUE)
ed_sites[is.na(sources_2015) & english, sources_2015 := 0L]

# Does the site appear in the 2012 data source?
ed_sites[english == TRUE, source_2012 := site_name %in% ed_sites_data[year == 2012, site_name]]

# For sites with non-distinct hospital names replace postcode in site_name with trust_name
ed_sites[, site_name := sub(" \\(.*$", paste0(" (", trust_name, ")"), site_name, perl = TRUE), by = site_name]

# Childrens A&E?
ed_sites[, children_only := grepl("(^| )childrens?($| )", hospital_name, perl = TRUE)]
ed_sites[site_name == "ormskirk and district general", children_only := TRUE]

# Is it an adult Type 1 A&E on yyyy-12-31?
year <- "2012"
yend_date <- as.Date(paste0(year, "-12-31"))
ed_sites[, paste0("type1_ae_", year) := TRUE]
ed_sites[children_only == TRUE | open_date > yend_date | closure_date <= yend_date, paste0("type1_ae_", year) := FALSE, with = FALSE]

year <- "2015"
yend_date <- as.Date(paste0(year, "-12-31"))
ed_sites[, paste0("type1_ae_", year) := TRUE]
ed_sites[children_only == TRUE | open_date > yend_date | closure_date <= yend_date, paste0("type1_ae_", year) := FALSE, with = FALSE]

# Save
save(ed_sites, file = "data/british ed sites.Rda")
