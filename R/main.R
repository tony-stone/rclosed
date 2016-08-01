genClosedMeasures <- function() {
  createCatchmentAreas()

  # Prepare HES data, several hours
  prepare_relevant_attendances()
  prepare_relevant_admitted_care()

  # A&E based measures, 5.5mins
  save_ed_attendances_measure()
  save_unnecessary_ed_attendances_measure()
  save_ed_attendances_admitted_measure()

  # APC based measures, 10.5mins
  save_emergency_admissions_measure()
  save_critical_care_stays_measure()
  save_length_of_stay_measure()

  # Deaths based measures, ??mins
  save_case_fatality_measure()

  #Ambulance based measures, ??mins
  save_ambulance_timings_measure()
  save_ambulance_red_calls_measure()
  save_ambulance_non_conveyance_measure()

}
