from config import gps_source_file, gps_url, gps_url_key
import json
import requests
import time

NOT_FOUND = -1
FROM_WEB = 0
FROM_LIST = 1


class GPSGenerator:
    coords = None
    get_from_web = False

    def __init__(self, get_from_web):
        if get_from_web is not None:
            self.get_from_web = get_from_web
        try:
            with open(gps_source_file, "r") as gps_file_content:
                self.coords = json.load(gps_file_content)
        except Exception as err:
            print('failed to load coordinates file', err)

    def update_coordinates_file(self, coordinates_file):
        first_line = True
        with open(coordinates_file, 'r') as source_file:
            for line in source_file.readlines():
                if first_line:
                    first_line = False
                else:
                    line_items = line.split(",")
                    street = ''.join([i for i in line_items[1] if not i.isdigit()]).strip()
                    city = ''.join([i for i in line_items[2] if not i.isdigit()]).strip()
                    address_key = f"{street}_{city}"
                    self.coords[address_key] = {
                        'lat': line_items[3],
                        'lng': line_items[4][:len(line_items[4])-1],
                        'street_accurate': 0
                    }
        self.save_gps_coords_file()


    @staticmethod
    def get_coordinates_from_file(source_file, target_file):
        file, gps_file = None, None
        gps_list = {}
        try:
            first_line = True
            with open(source_file, 'r') as file:
                for line in file.readlines():
                    if first_line:
                        first_line = False
                    else:
                        line_items = line.split(",")
                        street = ''.join([i for i in line_items[1] if not i.isdigit()]).strip()
                        city = ''.join([i for i in line_items[2] if not i.isdigit()]).strip()
                        address_key = "{}_{}".format(street, city)
                        if gps_list.get(address_key) is None:
                            gps_list[address_key] = {
                                'lat': line_items[3],
                                'lng': line_items[4][:len(line_items[4])-1]
                            }
                        #else:
                         #   print('{} was found in the existing list'.format(address_key))
            gps_json = json.dumps(gps_list, indent=4, ensure_ascii=False).encode('utf8')
            gps_file = open(target_file, 'w', encoding='utf-8')
            print(gps_json.decode(), file=gps_file)
        except Exception as err:
            print("failed to get coordinates from file ", err)
        finally:
            file.close()
            gps_file.close()
            #print('gps_coords written to file')

    def save_gps_coords_file(self):
        try:
            gps_json = json.dumps(self.coords, indent=4, ensure_ascii=False).encode('utf8')
            with open(gps_source_file, 'w', encoding='utf-8') as gps_file:
                print(gps_json.decode(), file=gps_file)
        except Exception as err:
            print('failed to save coordinates dict to file', err)

    def get_coords_from_list(self, street, city):
        lat, lng, accurate = 0,0,0
        street_city = f'{street}_{city}'
        if street_city in self.coords:
            lat = self.coords[street_city]['lat']
            lng = self.coords[street_city]['lng']
            if 'street_accurate' in self.coords[street_city]:
                accurate = self.coords[street_city]['street_accurate']
            else:
                accurate = -1
        return lat, lng, accurate

    def get_coordinates(self, street, city):
        lat, lng, accurate = self.get_coords_from_list(street, city)
        if lat is not 0:
            return f'{lat},{lng},{accurate}', FROM_LIST
        else:
            if self.get_from_web:
                lat, lng, accurate = self.get_coords_from_web(street, city)
                if lat is -1:
                    self.get_from_web = False
                elif lat is not 0:
                    self.coords[f'{street}_{city}'] = {
                        'lat': lat,
                        'lng': lng,
                        'street_accurate': accurate
                    }
                    return f'{lat},{lng},{accurate}', FROM_WEB
            lat, lng, accurate = self.get_coords_from_list(city, city)
            if lat is not 0:
                return f'{lat},{lng},{accurate}', FROM_LIST
            if self.get_from_web:
                lat, lng, accurate = self.get_coords_from_web(city, city)
                if lat is -1:
                    self.get_from_web = False
                elif lat is not 0:
                    self.coords[f'{city}_{city}'] = {
                        'lat': lat,
                        'lng': lng,
                        'street_accurate': accurate
                    }
                    return f'{lat},{lng},{int(accurate)}', FROM_WEB
            return '0,0,0', NOT_FOUND

    def load_gps_coordinates(self, bot_answers_file_path):
        gps_file_content = None
        data_with_coords = []
        from_list, from_web, not_found = 0, 0, 0
        try:
            gps_file_content = open(gps_source_file, "r")
            self.coords = json.load(gps_file_content)
            gps_file_content.close()
            first_line = True
            street_index = None
            city_index = None
            with open(bot_answers_file_path, 'r') as answers:
                for line in answers.readlines():
                    fields = line.split(',')
                    if first_line:
                        street_index = fields.index('street')
                        city_index = fields.index('city')
                        first_line = False
                    else:
                        coords_csv, source = self.get_coordinates(fields[street_index], fields[city_index])

                        if source == FROM_WEB:
                            from_web += 1
                        if source == FROM_LIST:
                            from_list += 1
                        if source == NOT_FOUND:
                            not_found += 1
                        data_with_coords.append(f'{line[:len(line)-1]},{coords_csv}' + '\n')
                        # coords_key = '{}_{}'.format(fields[street_index], fields[city_index])
                        # lat, lng, accurate = get_coords_from_list(fields[street_index], fields[city_index], coords)
                        # if lat == 0:
                        #     lat, lng, accurate = get_coords_from_web(fields[street_index], fields[city_index])
                        #     if lat == 0:
                        #         coords_key = '{}_{}'.format(fields[city_index], fields[city_index])
                        #         lat, lng, accurate = get_coords_from_list(fields[city_index], fields[city_index], coords)
                        #         if lat == 0:
                        #             lat, lng, accurate = get_coords_from_web(fields[city_index], fields[city_index])
                        #             if lat is not 0:
                        #                 coords[coords_key] = {
                        #                     'lat': lat,
                        #                     'lng': lng,
                        #                     'street_accurate': accurate
                        #                 }
                        #             else:
                        #                 addresses_from_web += 1
                        #         else:
                        #             addresses_from_list += 1
                        #     else:
                        #         addresses_from_web += 1
                        #         coords[coords_key] = {
                        #             'lat': lat,
                        #             'lng': lng,
                        #             'street_accurate': accurate
                        #         }
                        # else:
                        #     addresses_from_list += 1
                        # street_city = '{}_{}'.format(fields[street_index], fields[city_index])
                        # if street_city in coords:
                        #     addresses_from_list += 1
                        #     print('{} is a match!'.format(street_city))
                        #     lat = coords[street_city]['lat']
                        #     lng = coords[street_city]['lng']
                        #     if 'street_accurate' in coords[street_city]:
                        #         accurate = coords[street_city]['street_accurate']
                        #     else:
                        #         accurate = -1
                        # else:
                        #     addresses_from_web += 1
                        #     lat, lng, accurate = get_coords_from_web(fields[street_index], fields[city_index])
                        #     if lat == 0 and lng == 0:
                        #         if city in coords:
                        #             lat = coords[city]['lat']
                        #             lng = coords[city]['lng']
                        #             accurate = coords[city].get('street_accurate', 0)
                        #         else:
                        #             if ('{}_{}'.format(city, city)) in coords:
                        # if isinstance(lat, int) and lat < 0:
                        #     self.save_gps_coords_file(coords, gps_source_file)
                        #     print('failed to get gps data from web. Probably exceeded limits')
                        #     return data_with_coords
                        # accurate = int(accurate)
                        # data_with_coords.append(line[:len(line)-1] + ',{},{},{}\n'.format(lat, lng, accurate))
            self.save_gps_coords_file()
        except Exception as err:
            print('failed to load gps coordinates', err)
        finally:
            gps_file_content.close()
            print(f'addresses from memory: {from_list}, addresses from web-app: {from_web},'
                  f' addresses not found: {not_found}')
            return data_with_coords

    @staticmethod
    def get_coords_from_web(street, city):
        street_accurate = True
        if len(street) > 0:
            street_accurate = False
        response = requests.get(f'{gps_url}?key={gps_url_key}&address={street} {city}')
        if response.status_code == 200:
            response_object = response.json();
            if response_object['status'] is 'OK' and len(response_object['results'] > 0):
                location = response_object['results'][0]['geometry']['location']
                if location is not None:
                    return location['lat'], location['lng'], street_accurate
            else:
                return 0, 0, 0
        else:
            print('failed to get gps data from web, code received: ', response.status_code)
            print(response.content)
            return -1, -1, -1


if __name__ == '__main__':
    # source_filename = './known_locations.csv'
    # get_coordinates_from_file(source_filename, gps_source_file)
    city = 'מזכרת בתיה'
    street = 'חרצית'
    gps_generator = GPSGenerator(True)
    csv_line, source = gps_generator.get_coordinates(street, city)
    print(csv_line, source)
    # c, s = gps_generator.update_coordinates_file('./known_locations_new.csv')
    # print(c, s)
