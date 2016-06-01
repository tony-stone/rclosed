calcAllMeasures <- function() {
  # A&E, 5.5mins
  save_ed_attendances_measure()
  save_unnecessary_ed_attendances_measure()
  save_ed_attendances_admitted_measure()

  # APC admissions, 7mins
  save_emergency_admissions_measure()
  save_critical_care_stays_measure()
  save_length_of_stay_measure()

  # Deaths, 3.5mins
  #save_avoidable_deaths_measure()
  save_case_fatality_measure()
}
