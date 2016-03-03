library(data.table)
library(openxlsx)

# File: https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/6872/1871524.xls saved as *.xlsx
# Source: https://www.gov.uk/government/statistics/english-indices-of-deprivation-2010
# Retrieved: 29/01/2016
imd2010 <- data.table(read.xlsx("data-raw/DCLG/1871524.xlsx", sheet = "IMD 2010", startRow = 2, colNames = FALSE))

# Tidy data
imd2010[, c("X2", "X3", "X4", "X5", "X7") := NULL]
setnames(imd2010, c("LSOA", "imd_score"))

# Categorise into deciles
# Scores are such that most deprived has highest score; least deprived lowest score
deciles <- quantile(imd2010$imd_score, probs = seq(0, 1, by = 0.1), na.rm = TRUE, type = 8)
deciles[1] <- 0
deciles[11] <- Inf

# deciles are labeled such that most deprived is decile 1; least deprived decile 10
imd2010[, imd_decile := 11 - cut(imd_score, breaks = deciles, labels = FALSE)]

# Save
save(imd2010, file = "data/imd2010.Rda")
