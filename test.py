import json
from os import walk
from os import listdir
import os

NAMESPACE = 'com.housing.parquet.schema'
FOLDER_PATH = '/Users/shivamaggarwal/Documents/Housing_work/hadoop/events/dsl.tracking.configs/desktop/'
OUTPUT_FOLDER_PATH = '/Users/shivamaggarwal/Documents/Housing_work/hadoop/events/dsl.tracking.configs/schema/'
null = None

def convert_to_field_util(key, data, namespace):
    result = {}

    if type(data) is list:
        result['name'] = key + '_array'
        result['type'] = {}
        result['type']['type'] = 'array'
        for e in data:
            result['type']['items'] = convert_to_field_util(key, e, namespace)            
    elif type(data) is dict:
        # print data
        if (data.get("Nullable")):
            result['name'] = key
            result['type'] = "[\"null\", " +  + "]"
        else:
            result['name'] = key
            result['type'] = {}

            map_result = {}
            map_result['name'] = upper_case_name(key)
            map_result['namespace'] = namespace + '.' + key
            map_result['type'] = 'record'
            map_result['fields'] = []
            for k, v in data.iteritems():
                k = str(k)
                map_result['fields'].append(convert_to_field_util(k, v, map_result['namespace']))
            result['type'] = map_result
    elif type(data) is int:
        if (data > 2147483647):
            result['type'] = 'long'
        else:
            result['type'] = 'int'
        result['name'] = key
    elif type(data) is unicode:
        result['type'] = 'string'
        result['name'] = key
    elif type(data) is bool:
        result['type'] = 'boolean'
        result['name'] = key
    else:
        result['type'] = 'null'
        result['name'] = key

    return result

def convert_to_field(key, data):
    result = {}
    result['name'] = upper_case_name(key)
    result['namespace'] = NAMESPACE + '.' + key
    result['type'] = 'record'
    result['fields'] = []
    for k, v in data.iteritems():
        if k == '_cases':  
            for keys, values in v.iteritems():
                result_cases = {} 
                result_cases['name'] = keys
                result_cases['type'] = {}
                result_cases['type']['name'] = keys
                result_cases['type']['type'] = 'enum'
                result_cases['type']['symbols'] = []
                for first_key in values:
                    result_cases['type']['symbols'].append(first_key)
                result['fields'].append(result_cases)
        else:
            # if (k != 'clid' and k != 'previous_url'):
            # print k, v
            result['fields'].append(convert_to_field_util(k, v, result['namespace']))
    return result

def write(data, path):
    with open(path, 'w') as f:
        json.dump(data, f)

def get_data(filename):
    data = None
    with open(filename) as f:
        data = json.loads(f.read())
    return data

def handle_imports(data):
    result_optional = {}
    result = {}

    list_optional_imports = ['serach_info', 'campaign_info', 'experiments']

    # adding data in import files
    for k, v in data.iteritems():
        k = str(k)
        if k == '_imports':
            for e in v:
                if e in list_optional_imports:
                    e = str(e)
                    list_e = e.split('.')
                    import_data = get_data(FOLDER_PATH + list_e[0]+'.json')
                    if (len(list_e) > 1):
                        del list_e[0]
                        for key in list_e:
                            result_optional[key] = import_data[key]
                    # importing all the keys from _imports files
                    else:
                        for key, value in import_data.iteritems():
                            key = str(key)
                            result_optional[key] = value
                else:
                    e = str(e)
                    # importing specific key from _imports files
                    list_e = e.split('.')
                    import_data = get_data(FOLDER_PATH + list_e[0]+'.json')
                    if (len(list_e) > 1):
                        del list_e[0]
                        for key in list_e:
                            result[key] = import_data[key]
                            if key in data:
                                result[key] = data[key]
                    # importing all the keys from _imports files
                    else:
                        for key, value in import_data.iteritems():
                            key = str(key)
                            result[key] = value
                            if key in data:
                                result[key] = data[key]
        else:
            result[k] = v
    return result, result_optional

def handle_cases(data):
    # removing key names which match _cases names 
    result = []
    for e in data['fields']:
        if (type(e['type']) is dict):
            name = e['name']
            for element in data['fields']:
                if (element['name'] == name) and (type(element['type']) is not dict):
                    result.append(element)
    for e in result:
        data['fields'].remove(e)
    return data

def upper_case_name(name):
    uppername = ''
    for i in range (0, len(name)):
        if i == 0:
            uppername += name[i].upper()
        elif (name[i-1] == '_'):
            uppername += name[i].upper()
        else:
            uppername += name[i]
    uppername = uppername.replace('_', '')
    return uppername

def add_optional_fields_util(key, data, namespace):
    result = {}

    if type(data) is list:
        result['name'] = key + '_array'
        result['type'] = {}
        result['type']['type'] = ['null', 'array']
        for e in data:
            result['type']['items'] = convert_to_field_util(key, e, namespace)            
    elif type(data) is dict:
        # print data
    
        result['name'] = key
        result['type'] = {}

        map_result = {}
        map_result['name'] = upper_case_name(key)
        map_result['namespace'] = namespace + '.' + key
        map_result['type'] = ['null', 'record']
        map_result['default'] = null
        map_result['fields'] = []
        for k, v in data.iteritems():
            k = str(k)
            map_result['fields'].append(convert_to_field_util(k, v, map_result['namespace']))
        result['type'] = map_result        
    elif type(data) is int:
        result['type'] = ['null', 'int']
        result['name'] = key
    elif type(data) is unicode:
        result['type'] = ['null', 'string']
        result['name'] = key
    elif type(data) is bool:
        result['type'] = ['null', 'boolean']
        result['name'] = key
    else:
        result['type'] = 'null'
        result['name'] = key

    result['default'] = null

    return result


def add_optional_fields(data, data_optional):
    for k, v in data_optional.iteritems():
        result = add_optional_fields_util(k, v, NAMESPACE + '.' + k)
    # print result
        data['fields'].append(result)
    # return result

def process(infilename):
    # opening file for reading 
    data = None
    with open(infilename) as f:
        try:
            data = json.loads(f.read())
        except ValueError:
            print "Not valid json"

    # handling imports field in schema
    if data is not None:
        try:
            data, data_optional = handle_imports(data)
            # print data
        except Exception as e:
            print "Values in _cases do not match in file: " + infilename
            print str(e)

        # defining path of the output file
        category = data['category']
        action = data['action']
        outfilename = OUTPUT_FOLDER_PATH + category + '#' + action + '.avsc'
        key = category + '_' + action


        # getting schema for input file
        data = convert_to_field(key, data)
        add_optional_fields(data, data_optional)
        data = handle_cases(data)

        # write schema in output file 
        write(data, outfilename)

def generate_schema():
    # iterate over all the json files
    # f = []
    # for f in listdir(FOLDER_PATH):
    #     if os.path.isdir(FOLDER_PATH + f):
    #         file_path = FOLDER_PATH + f + '/' 
    #         for filename in listdir(file_path):
    #             if ('.json' in filename):
    #                 process(file_path + filename)

    process("/Users/shivamaggarwal/Desktop/impressions_rent.json")
if __name__ == "__main__":
    generate_schema()
