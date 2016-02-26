library("data.table")

# Script to generate list of 2001 LSOAs that intersect with our PCTs of interest

# Read in OA 2011 to PCT 2011 lookup
oa11_to_pct11 <- fread(input = "data-raw/geography data/Lookups/Output_areas_(2011)_to_primary_care_organisations_(2011)_to_strategic_health_authorities_(2011)_E+W_lookup/OA11_PCO11_SHA11_EW_LU.csv", header = TRUE, sep = ",", colClasses = "character", stringsAsFactors = FALSE)
# Read in OA 2001 to OA 2011 lookup
oa01_to_oa11 <- fread(input = "data-raw/geography data/Lookups/Output_areas_(2001)_to_output_areas_(2011)_to_local_authority_districts_(2011)_E+W_lookup/OA01_OA11_LAD11_EW_LU.csv", header = TRUE, sep = ",", colClasses = "character", stringsAsFactors = FALSE)

# Prepare for join on OA 2011 code
setkey(oa11_to_pct11, OA11CD)
setkey(oa01_to_oa11, OA11CD)

# Do join: PCT 2011 to OA 2001
pct11_to_oa01 <- oa01_to_oa11[oa11_to_pct11, allow.cartesian = TRUE]

# Remove unnecesarry fields
pct11_to_oa01[, c("OA01CD", "CHGIND", "LAD11CD", "LAD11NM", "LAD11NMW", "PCO11NMW", "OA11PERCENT", "SHA11CD", "SHA11NM", "OA11PERCENT1") := NULL]

# Remove OA11's which have no OA01 equivalent
pct11_to_oa01 <- pct11_to_oa01[!is.na(OA01CDO),]

# Read in  OA01 to LSOA01 lookup
oa01_to_lsoa01 <- fread(input = "data-raw/geography data/Lookups/Output_areas_(2001)_to_lower_layer_super_output_areas_(2001)_to_middle_layer_super_output_areas_(2001)_E+W_lookup/OA01_LSOA01_MSOA01_EW_LU.csv", header = TRUE, sep = ",", colClasses = "character", stringsAsFactors = FALSE)

# Prepare for join on OA 2001 (old style) code
setkey(pct11_to_oa01, OA01CDO)
setkey(oa01_to_lsoa01, OA01CD)

# Do join: PCT 2011 to LSOA 2001 data
pct11_to_lsoa01 <- oa01_to_lsoa01[pct11_to_oa01]

# Remove unnecesarry fields
pct11_to_lsoa01[, c("MSOA01CD", "MSOA01NM", "OA11CD", "OA01CD") := NULL]

# Keep distinct LSOA 2001s which have at least one OA in a PCT 2011 (this is not 1-to-1)
setkeyv(pct11_to_lsoa01, c("LSOA01CD", "PCO11CD"))
pct11_to_lsoa01 <- unique(pct11_to_lsoa01)

# Remove unnecesarry data
rm(oa01_to_oa11, oa11_to_pct11, pct11_to_oa01)
gc()

# List of English PCTs to choose from
# paste0("'", paste0(sort(unique(pct11_to_lsoa01$PCO11NM[substr(pct11_to_lsoa01$LSOA01CD, 1, 1) == "E"])), collapse = "', '"), "'")

# choose from
# 'Ashton, Leigh and Wigan', 'Barking and Dagenham', 'Barnet', 'Barnsley', 'Bassetlaw', 'Bath and North East Somerset',
# 'Bedfordshire', 'Berkshire East', 'Berkshire West', 'Bexley', 'Birmingham East and North',
# 'Blackburn with Darwen Teaching', 'Blackpool', 'Bolton Teaching', 'Bournemouth and Poole Teaching',
# 'Bradford and Airedale Teaching', 'Brent Teaching', 'Brighton and Hove City', 'Bristol', 'Bromley',
# 'Buckinghamshire', 'Bury', 'Calderdale', 'Cambridgeshire', 'Camden', 'Central and Eastern Cheshire',
# 'Central Lancashire', 'City and Hackney Teaching', 'Cornwall and Isles of Scilly', 'County Durham',
# 'Coventry Teaching', 'Croydon', 'Cumbria Teaching', 'Darlington', 'Derby City', 'Derbyshire County', 'Devon',
# 'Doncaster', 'Dorset', 'Dudley', 'Ealing', 'East Lancashire Teaching', 'East Riding of Yorkshire',
# 'East Sussex Downs and Weald', 'Eastern and Coastal Kent', 'Enfield', 'Gateshead', 'Gloucestershire',
# 'Great Yarmouth and Waveney', 'Greenwich Teaching', 'Halton and St Helens', 'Hammersmith and Fulham', 'Hampshire',
# 'Haringey Teaching', 'Harrow', 'Hartlepool', 'Hastings and Rother', 'Havering', 'Heart of Birmingham Teaching',
# 'Herefordshire', 'Hertfordshire', 'Heywood, Middleton and Rochdale', 'Hillingdon', 'Hounslow', 'Hull Teaching',
# 'Isle of Wight National Health Service', 'Islington', 'Kensington and Chelsea', 'Kingston', 'Kirklees', 'Knowsley',
# 'Lambeth', 'Leeds', 'Leicester City', 'Leicestershire County and Rutland', 'Lewisham', 'Lincolnshire Teaching',
# 'Liverpool', 'Luton', 'Manchester Teaching', 'Medway', 'Mid Essex', 'Middlesbrough', 'Milton Keynes', 'Newcastle',
# 'Newham', 'Norfolk', 'North East Essex', 'North East Lincolnshire', 'North Lancashire Teaching', 'North Lincolnshire',
# 'North Somerset', 'North Staffordshire', 'North Tyneside', 'North Yorkshire and York', 'Northamptonshire Teaching',
# 'Northumberland', 'Nottingham City', 'Nottinghamshire County Teaching', 'Oldham', 'Oxfordshire', 'Peterborough',
# 'Plymouth Teaching', 'Portsmouth City Teaching', 'Redbridge', 'Redcar and Cleveland', 'Richmond and Twickenham',
# 'Rotherham', 'Salford', 'Sandwell', 'Sefton', 'Sheffield', 'Shropshire County', 'Solihull', 'Somerset',
# 'South Birmingham', 'South East Essex', 'South Gloucestershire', 'South Staffordshire', 'South Tyneside',
# 'South West Essex', 'Southampton City', 'Southwark', 'Stockport', 'Stockton-on-Tees Teaching', 'Stoke on Trent',
# 'Suffolk', 'Sunderland Teaching', 'Surrey', 'Sutton and Merton', 'Swindon', 'Tameside and Glossop',
# 'Telford and Wrekin', 'Torbay', 'Tower Hamlets', 'Trafford', 'Wakefield District', 'Walsall Teaching',
# 'Waltham Forest', 'Wandsworth', 'Warrington', 'Warwickshire', 'West Essex', 'West Kent', 'West Sussex',
# 'Western Cheshire', 'Westminster', 'Wiltshire', 'Wirral', 'Wolverhampton City', 'Worcestershire'

# Set of PCTs of interest
pcts_of_interest <- c("Ashton, Leigh and Wigan", "Bassetlaw", "Bedfordshire", "Berkshire East", "Berkshire West", "Blackburn with Darwen Teaching",
                      "Bolton Teaching", "Buckinghamshire", "Bury", "Calderdale", "Central Lancashire", "Cornwall and Isles of Scilly",
                      "County Durham", "Coventry Teaching", "Cumbria Teaching", "Darlington", "Derbyshire County", "Devon", "Doncaster", "Dorset",
                      "Hertfordshire","Central and Eastern Cheshire", "East Lancashire Teaching", "East Riding of Yorkshire", "Gloucestershire",
                      "Halton and St Helens", "Hampshire", "Hartlepool", "Kirklees", "Leicestershire County and Rutland", "Lincolnshire Teaching",
                      "Luton", "Manchester Teaching", "Middlesbrough", "North East Lincolnshire", "North Lincolnshire", "North Staffordshire",
                      "North Yorkshire and York", "Northumberland", "Nottingham City", "Nottinghamshire County Teaching", "Oldham", "Oxfordshire",
                      "Redcar and Cleveland", "Heywood, Middleton and Rochdale", "Rotherham", "Salford", "Sefton", "Sheffield", "Solihull",
                      "Somerset", "Stockport", "Stockton-on-Tees Teaching", "Surrey", "Trafford", "Warrington", "Warwickshire", "Western Cheshire",
                      "Wiltshire", "Worcestershire")

# Check we have 60 PCTs
stopifnot(length(pcts_of_interest) == 60)

# Get all distinct English LSOA 2001's that are in any selected PCT
lsoa01s_in_HES <- sort(unique(pct11_to_lsoa01[PCO11NM %in% pcts_of_interest & substr(LSOA01CD, 1, 1) == "E", LSOA01CD]))

# Save our sorted list of LSOA 2001s for which we should have HES data
save(lsoa01s_in_HES, file = "data/lsoa 2001s in hes data.rda")
