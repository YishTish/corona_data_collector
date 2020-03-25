from config import gps_source_file
import json
import requests
import time


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


def save_gps_coords_file(coordinates_data, coordinates_file):
    gps_file = None
    try:
        gps_json = json.dumps(coordinates_data, indent=4, ensure_ascii=False).encode('utf8')
        gps_file = open(coordinates_file, 'w', encoding='utf-8')
        print(gps_json.decode(), file=gps_file)
    except Exception as err:
        print('failed to save coordinates dict to file', err)
    finally:
        gps_file.close()

def get_coords_from_list(street, city, list):
    lat, lng, accurate = 0,0,0
    street_city = '{}_{}'.format(street, city)
    if street_city in list:
        #print('{} is a match!'.format(street_city))
        lat = list[street_city]['lat']
        lng = list[street_city]['lng']
        if 'street_accurate' in list[street_city]:
            accurate = list[street_city]['street_accurate']
        else:
            accurate = -1
    return lat, lng, accurate


def load_gps_coordinates(bot_answers_file):
    gps_file_content = None
    addresses_from_list, addresses_from_web = 0, 0
    data_with_coords = []
    try:
        gps_file_content = open(gps_source_file, "r")
        coords = json.load(gps_file_content)
        gps_file_content.close()
        first_line = True
        street_index = None
        city_index = None
        with open(bot_answers_file, 'r') as answers:
            for line in answers.readlines():
                fields = line.split(',')
                if first_line:
                    street_index = fields.index('street')
                    city_index = fields.index('city')
                    first_line = False
                else:
                    coords_key = '{}_{}'.format(fields[street_index], fields[city_index])
                    lat, lng, accurate = get_coords_from_list(fields[street_index], fields[city_index], coords)
                    if lat == 0:
                        lat, lng, accurate = get_coords_from_web(fields[street_index], fields[city_index])
                        if lat == 0:
                            coords_key = '{}_{}'.format(fields[city_index], fields[city_index])
                            lat, lng, accurate = get_coords_from_list(fields[city_index], fields[city_index], coords)
                            if lat == 0:
                                lat, lng, accurate = get_coords_from_web(fields[city_index], fields[city_index])
                                if lat is not 0:
                                    coords[coords_key] = {
                                        'lat': lat,
                                        'lng': lng,
                                        'street_accurate': accurate
                                    }
                                else:
                                    addresses_from_web += 1
                            else:
                                addresses_from_list += 1
                        else:
                            addresses_from_web += 1
                            coords[coords_key] = {
                                'lat': lat,
                                'lng': lng,
                                'street_accurate': accurate
                            }
                    else:
                        addresses_from_list += 1
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
                    if isinstance(lat, int) and lat < 0:
                        save_gps_coords_file(coords, gps_source_file)
                        print('failed to get gps data from web. Probably exceeded limits')
                        return data_with_coords
                    accurate = int(accurate)
                    data_with_coords.append(line[:len(line)-1] + ',{},{},{}\n'.format(lat, lng, accurate))
        save_gps_coords_file(coords, gps_source_file)
    except Exception as err:
        print('failed to load gps coordinates', err)
    finally:
        gps_file_content.close()
        print('addresses from memory: {}, addresses from web-app: {}'.format(addresses_from_list, addresses_from_web))
        return data_with_coords


def get_coords_from_web(street, city):
    time.sleep(1)
   # print('getting gps data from web for {} {}'.format(street, city))
    query_params = 'city={}'.format(city)
    street_accurate = True
    if len(street) > 0:
        query_params += '&street={}'.format(street)
        street_accurate = False
    response = requests.get('https://nominatim.openstreetmap.org/search?{}&country=Israel&format=json'
                            .format(query_params))
    if response.status_code == 200:
        respone_object = response.json();
        if len(respone_object) > 0:
            #print('got address details from web {}, {}, {}'.format(respone_object[0]['lat'], respone_object[0]['lon'],
                        #                                           street_accurate))
            return respone_object[0]['lat'], respone_object[0]['lon'], street_accurate
        else:
            if len(street) > 0:
                return get_coords_from_web('', city)
            else:
            #    print('no gps data found on web')
                return 0, 0, 0
    else:
        print('failed to get gps data from web, code received: ', response.status_code)
        print(response.content)
        return -1, -1, -1


if __name__ == '__main__':
    # source_filename = './known_locations.csv'
    # get_coordinates_from_file(source_filename, gps_source_file)
    city = 'ירושלים'
    street = 'אבן שמואל'
    latitude, longitude, with_street = get_coords_from_web(street, city)
    print(latitude, longitude)
