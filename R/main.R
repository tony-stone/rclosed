genClosedMeasures <- function() {
  createCatchmentAreas()

  # Prepare HES data, several hours
  prepare_relevant_attendances()
  prepare_relevant_admitted_care()

  # A&E based measures, 7mins
  save_ed_attendances_measure()
  save_unnecessary_ed_attendances_measure()
  save_ed_attendances_admitted_measure()

  # APC based measures, 9mins
  save_emergency_admissions_measure()
  save_critical_care_stays_measure()
  save_hospital_transfers_measure()
  save_length_of_stay_measure()

  # Deaths based measures, 22mins
  link_hes_apc_and_death_data()
  save_deaths_measure()
  save_case_fatality_measure()

  #Ambulance based measures, 6mins
  save_ambulance_red_calls_measures()
  save_ambulance_green_calls_measures()
  save_ambulance_all_calls_measure()
}
