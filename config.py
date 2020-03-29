from gps_url_key import gps_url_key
db_settings = {
    "host": "35.230.137.198",
    "port": 5432,
    "username": "readonly",
    "password": "riePoo7phohgaiPh"
}
answer_titles = {
    'id': 'id',
    'created': 'timestamp',
    'age': 'age',
    'sex': 'gender',
    'street': 'street',
    'city_town': 'city',
    'chronic_diabetes': 'diabetes',
    'chronic_hypertension': 'hypertension',
    'chronic_ischemic_heart_disease_or_stroke': 'ischemic_heart_disease_or_stroke',
    'chronic_lung_disease': 'lung_disease',
    'chronic_cancer': 'cancer',
    'chronic_kidney_failure': 'kidney_failure',
    'chronic_immune_system_suppression': 'immune_system_suppression',
    'smoking': 'smoking',
    'insulation_status': 'isolation',
    'insulation_start_date': 'isolation_start_date',
    'insulation_reason': 'isolation_reason',
    'insulation_returned_from_abroad_date': 'isolation_returned_from_abroad_date',
    'insulation_patient_number': 'insulation_patient_number',
    'insulation_exposure_date': 'insulation_exposure_date',
    'isolated_with_family': 'isolated_with_family',
    'diagnosed_location': 'diagnosed_location',
    'general_feeling': 'general_feeling',
    'current-report-temperature': 'current-report-temperature',
    'temperature': 'temperature',
    'toplevel_symptoms_cough': 'toplevel_symptoms_cough',
    'toplevel_symptoms_pains': 'toplevel_symptoms_pains',
    'toplevel_symptoms_tiredness': 'toplevel_symptoms_tiredness',
    'toplevel_symptoms_stomach': 'toplevel_symptoms_stomach',
    'symptoms_clogged_nose': 'symptoms_clogged_nose',
    'symptoms_sore_throat': 'symptoms_sore_throat',
    'symptoms_dry_cough': 'symptoms_dry_cough',
    'symptoms_moist_cough': 'symptoms_moist_cough',
    'symptoms_breath_shortness': 'symptoms_breath_shortness',
    'symptoms_muscles_pain': 'symptoms_muscles_pain',
    'symptoms_headache': 'symptoms_headache',
    'symptoms_fatigue': 'symptoms_fatigue',
    'symptoms_infirmity': 'symptoms_infirmity',
    'symptoms_diarrhea': 'symptoms_diarrhea',
    'symptoms_nausea_and_vomiting': 'symptoms_nausea_and_vomiting',
    'symptoms_chills': 'symptoms_chills',
    'symptoms_confusion': 'symptoms_confusion',
    'symptoms_tiredness_or_fatigue': 'symptoms_tiredness_or_fatigue',
    'symptoms_smell_taste_loss': 'symptoms_smell_taste_loss',
    'symptoms_other': 'symptoms_other',
    'exposure_met_people': 'exposure_met_people',
    'flatmates': 'flatmates',
    'flatmates_over_70': 'flatmates_over_70',
    'flatmates_under_18': 'flatmates_under_18',
    'work_serve_over_10': 'work_serve_over_10',
    'met_under_18': 'met_under_18',
    'met_above_18': 'met_above_18',
    'numPreviousReports': 'num_previous_reports',
}

keys_to_convert = {
    'exposure_status': 'insulation_status',
    'insulation_reason': 'insulation_status',
    'hospitalized': 'diagnosed_location',
}

values_to_convert = {
    'sex': {
        'male': 0,
        'female': 1
    },
    'smoking': {
        'אף פעם': 0,
        'never': 0,
        'עישנתי בעבר, לפני יותר מחמש שנים': 1,
        'long past smoke': 1,
        'long_past_smokre': 1,
        'עישנתי בעבר, הפסקתי לפני פחות מחמש שנים': 2,
        'עישנתי בעבר, בחמש השנים האחרונות': 2,
        'short_past_smoker': 2,
        'עישון יומיומי': 3,
        'daily_smoker': 3,
    },
    'insulation_status': {
        'not-insulated': 0,
        'none': 0,
        'voluntary': 1,
        'back-from-abroad': 2,
        'contact-with-patient': 3,
        'has-symptoms': 4,
        'has_symptoms': 4,
        'hospitalized': 5,
        'diagnosed': 5,
    },
    'diagnosed_location': {
        'none': 0,
        'home': 1,
        'hotel': 2,
        'hospital_from_corona_lab': 3,
        'hospital': 4,
        'recovered': 5,
    },
    'general_feeling': {
        'feel_good': 0,
        'feel_bad': 1,
    }
}

gps_source_file = './gps_data.json'
gps_url = 'https://maps.googleapis.com/maps/api/geocode/json'
gps_url_key = gps_url_key
use_gps_finder = False
query_batch_size = 10000
process_max_rows = 100000
