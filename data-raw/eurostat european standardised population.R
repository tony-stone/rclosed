library(data.table)

# See "EUROSTAT ESP/European Standard Population 2013 Report KS-RA-13-028-EN.pdf" Annex F (page 121)

standardised_population <- data.table(age_cat = factor(c("[0,1)", "[1,5)", "[5,10)", "[10,15)", "[15,20)", "[20,25)", "[25,30)",
  "[30,35)", "[35,40)", "[40,45)", "[45,50)", "[50,55)", "[55,60)", "[60,65)",
  "[65,70)", "[70,75)", "[75,80)", "[80,85)", "[85,90)", "[90,Inf)"), ordered = TRUE),
  population = c(1000, 4000, 5500, 5500, 5500, 6000, 6000, 6500, 7000, 7000, 7000,
    7000, 6500, 6000, 5500, 5000, 4000, 2500, 1500, 1000))

save(standardised_population, file = "data/standardised population.Rda", compress = "gzip")
