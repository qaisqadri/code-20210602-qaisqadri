import json
import pandas as pd
import math

PARALLELIZE = False

if PARALLELIZE:
    from pandarallel import pandarallel
    NUM_OF_CORES = 12
    pandarallel.initialize(nb_workers = NUM_OF_CORES)

# the mapping representing BMI category with BMI value range and health risk
MAPPING = {
    "Underweight"            : {'lower' : -math.inf, 'upper' : 18.49,    "risk" : 'Malnutrition Risk'},
    "Normal Weight"          : {'lower' : 18.50,     'upper' : 24.99,    "risk" : 'Low Risk'},
    "Overweight"             : {'lower' : 25,        'upper' : 29.99,    "risk" : 'Enhanced Risk'},
    "Moderately Obese"       : {'lower' : 30,        'upper' : 34.99,    'risk' : 'Medium Risk'},
    "Severely Obese"         : {'lower' : 35,        'upper' : 39.99,    'risk' : 'High Risk'},
    "Very Severly Obese"     : {'lower' : 40,        'upper' : math.inf, 'risk' : 'Very High Risk'}
    }

def cal_bmi(height, weight, cm=True):
    """Function to Calculate the BMI value based on formula, bmi = ( weight(kg)/(height(m) * height(m)) )
    returns BMI value, (in case of height or weight being passed in as 0, returns BMI as 0.0)
    Parameters:
    height (int or float): expected in centimeters, but if passed in meters then pass in cm = False
    weight (int or float): expected in kgs
    cm (bool): param to define the unit of input height, default = True, meaning by default this function accepts height in centimeters
    Returns:
    bmi value (float) : the calculated BMI value
   
    """

    if cm:
        height /= 100
    try:
        bmi = round(weight / (height**2), 2)
    except ZeroDivisionError as e:
        bmi = 0.0
    return bmi


def get_category_and_risk(bmi_value):
    """Function to Map the BMI Category and Health risk based on input bmi_value.
    Parameters:
    bmi_value (float): the bmi value
    Returns: a tuple containing:
    BMI category : the BMI category
    Health risk : the health risk

    """
    for category, range_risk in MAPPING.items():
        
        lower_limit = range_risk['lower']
        upper_limit = range_risk['upper']
        risk = range_risk['risk']
        
        if lower_limit <= bmi_value <= upper_limit:
            
            return category, risk


def generate_data(row):
    """Function to generate the BMI value and BMI category and Health value given required input in python dict or pandas Series.
    Helpful for generating the data when data is in pandas dataframe
    Calls functions cal_bmi and get_category_and_risk

    Parameters:
    row (dict or pandas Series): the input data in dict or series with fields required fields HeightCm, WeightKg
    Returns: python dict or pandas Series with BMI value, BMI Category, Health risk in addition to original fields
    row (dict or pandas Series):

    """
    height_cm = row['HeightCm']
#     height_m = height_cm/100
    weight_kg = row["WeightKg"]
    
    bmi = cal_bmi(height_cm, weight_kg )
    
    category, risk = get_category_and_risk(bmi)
    
    row['BMI value'] = bmi
    row['BMI Category'] = category
    row['Health risk'] = risk
    
    return row


def get_counts_from_ranges(df, category):
    """Given the Pandas dataFrame with calculated BMI values, BMI category and Health risk, 
    and given a BMI category value returns the number of people or entries for that particular category.

    Parameters:
    df (pandas dataframe): the data.
    category (str) : the BMI category value to query the number of people or entries.
    Returns: 
    count (the category count): number of entries or people for the given category, if category value not present then 0 is returned.

    """

    category_count = 0
    if category not in MAPPING:
        return 0
    for bmi_value in df['BMI value'].values:
        ranges_risk_dict = MAPPING.get(category)
        
        lower_limit = ranges_risk_dict['lower']
        upper_limit = ranges_risk_dict['upper']
            
        if lower_limit <= bmi_value <= upper_limit:
            
            category_count+=1
    return category_count


def verify_count(df, category, count):
    """Given the Pandas dataFrame with calculated BMI values, BMI category and Health risk, 
    and given a BMI category value, and
    given a count for the category, verify whether the count is correct or not

    Parameters:
    df (pandas dataframe): the data.
    category (str) : the BMI category value to query the number of people or entries.
    count (the category count): number of entries or people for the given category, the count to be verified in data for the category.
    Retuns:
    result as bool: True if correct else False
    """
    count_from_data = df.loc[df['BMI Category'] == category].shape[0]

    if count_from_data == count:
        return True
    else:
        return False
    

def main(json_file_path, result_file_type = 'csv'):
    """Function using the defined functions to implement the BMI calculation and other operations
    Parameters:
    json_file_path (str) : the path to json file containing data
    result_file_type (str) : the type of result expected. Two options : csv or json, default value is csv. if "json" is passed then result is stored in json file
    the name of result file will be result(.json or .csv) in the current location.

    """
    # 1) Calculate the BMI (Body Mass Index) using ​ Formula 1​ , BMI Category and Health
    # risk ​from Table 1​ of the person and add them as 3 new columns
    with open(json_file_path, 'r') as fp:
        data = json.load(fp)

    df = pd.DataFrame(data)

    if not PARALLELIZE:
        # run in single core mode
        df = df.apply(generate_data, axis = 1)
    else:
        # run in parallel mode
        df = df.parallel_apply(generate_data, axis=1)

    if result_file_type == 'json':
        file_name = "result.csv"
        df.to_csv(file_name, index=False)
        
    else:
        file_name = "result.json"
        df.to_json(file_name, orient = 'records', indent = 4)

    print(f"data saved : {file_name}")

    # 2) Count the total number of overweight people using ranges in the column BMI
    # Category of ​ Table 1,​ check this is consistent programmatically and add any other
    # observations in the documentation 

    check_category = 'Overweight'

    counts = get_counts_from_ranges(df, check_category)

    print(f'The counts for {check_category} : {counts}')

    result = verify_count(df, check_category, counts)
    if result:
        print(f'Verified counts for {check_category} and Success')
    else:
        print(f"Counts verification incorrect! for {check_category}")



if __name__ == '__main__':

    # json_file_path = 'big_json.json'
    json_file_path = 'json_data.json'
    result_file_type = 'csv' #or json
    # main(json_file_path)
    # main(json_file_path) # by default result file will be csv
    main(json_file_path, result_file_type = result_file_type ) 

