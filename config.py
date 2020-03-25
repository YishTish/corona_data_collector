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
    'smoking': 'smoking',
    'insulation_status': 'isolation',
    'insulation_patient_number': 'insulation_patient_number',
    'insulation_exposure_date': 'insulation_exposure_date',
    'toplevel_symptoms_cough': 'toplevel_symptoms_cough',
    'toplevel_symptoms_pains': 'toplevel_symptoms_pains',
    'toplevel_symptoms_tiredness': 'toplevel_symptoms_tiredness',
    'toplevel_symptoms_stomach': 'toplevel_symptoms_stomach',
    'symptoms_clogged_nose': 'symptoms_clogged_nose',
    'symptoms_sore_throat': 'symptoms_sore_throat',
    'symptoms_dry_cough': 'symptoms_dry_cough',
    'symptoms_moist_cough': 'symptoms_moist_cough',
    'symptoms_breath_shortness':'symptoms_breath_shortness',
    'symptoms_muscles_pain': 'symptoms_muscles_pain',
    'symptoms_headache': 'symptoms_headache',
    'symptoms_fatigue': 'symptoms_fatigue',
    'symptoms_infirmity': 'symptoms_infirmity',
    'symptoms_diarrhea': 'symptoms_diarrhea',
    'symptoms_nausea_and_vomiting': 'symptoms_nausea_and_vomiting',
    'symptoms_other': 'symptoms_other',
    'exposure_met_people': 'exposure_met_people',
    'flatmates': 'flatmates',
    'flatmates_over_70': 'flatmates_over_70',
    'flatmates_under_18': 'flatmates_under_18',
    'work_serve_over_10': 'work_serve_over_10',
}

values_to_convert = {
    'sex': {
        'male': 0,
        'female': 1
    },
    'smoking': {
        'אף פעם': 0,
        'עישנתי בעבר, לפני יותר מחמש שנים': 1,
        'עישנתי בעבר, בחמש השנים האחרונות': 2,
        'עישון יומיומי': 3
    },
    'insulation_status': {
        'not-insulated': 0,
        'voluntary': 1,
        'back-from-abroad': 2,
        'contact-with-patient': 3,
        'has-symptoms': 4,
        'hospitalized': 5
    }
}

gps_source_file = './gps_data.json'
