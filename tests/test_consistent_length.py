import unittest
from DBToFileWriter import DBToFileWriter, collect_row


class TestConsistentLength(unittest.TestCase):

    def test_length_consistent(self):
        data = [{
            "age": "66",
            "sex": "female",
            "locale": "he",
            "street": "קהילת ונציה",
            "smoking": "long_past_smokre",
            "version": "1.0.1",
            "city_town": "תל אביב,נאות אפקה",
            "temperature": "36.5",
            "met_above_18": "1",
            "met_under_18": "0",
            "chronic_cancer": True,
            "exposure_status": "none",
            "general_feeling": "feel_good",
            "numPreviousReports": 0,
            "chronic_hypertension": True,
            "id": 172495,
            "created": "2020-03-25T15:04:39.768496"},
            {"id": 175130, "created": "2020-03-25 16:32:38.997021", "age": "72", "sex": "male", "locale": "he", "street": "יונה סאלק", "smoking": "long_past_smokre", "version": "1.0.1", "city_town": "אשדוד", "temperature": "36.3", "met_above_18": "0", "met_under_18": "0", "exposure_status": "none", "general_feeling": "feel_good", "numPreviousReports": 0, "chronic_hypertension": True},
            {"id": 175131, "created": "2020-03-25 16:32:39.75928", "age": "26", "sex": "female", "locale": "he", "street": "מבצע דקל", "smoking": "אף פעם", "version": "0.2.1", "continue": "בטח!", "city_town": "פתח תקווה", "flatmates": "3", "flatmates_over_70": "0", "insulation_status": "not-insulated", "flatmates_under_18": "1", "work_serve_over_10": False, "exposure_met_people": "0", "symptoms_sore_throat": True, "symptoms_clogged_nose": True, "toplevel_symptoms_cough": True}
        ]
        db_to_file_writer = DBToFileWriter()
        db_to_file_writer.target_filename = 'test_file.csv'
        db_to_file_writer.resultSet = data
        db_to_file_writer.write_answer_keys()
        db_to_file_writer.log_database_data()
        with open("./test_file.csv", "r") as test_file:
            first_line = test_file.readline()
            first_line_items = first_line.split(',')
            line = test_file.readline()
            while line:
                line_items = line.split(',')
                self.assertEqual(len(first_line_items), len(line_items), f'line is not same lengths as title {line}')
                line = test_file.readline()

    def test_exposure_status_failure(self):
        record = {
            'age': '72', 'sex': 0, 'locale': 'he', 'street': 'יונה סאלק', 'smoking': 1, 'version': '1.0.1',
            'city_town': 'אשדוד', 'temperature': '36.3', 'met_above_18': '0', 'met_under_18': '0',
            'general_feeling': 0, 'numPreviousReports': 0, 'chronic_hypertension': 1, 'id': 175130,
            'created': '2020-03-25T16:32:38.997021', 'insulation_status': 0
         }
        record_to_store = collect_row(record)
        print(record_to_store)
        self.assertGreater(len(record_to_store), 0, 'failed to create a record that can be stored in file')
