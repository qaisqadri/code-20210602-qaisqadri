"""
Defines unit tests for the "calculate_bmi" module.
"""
# 3) Create build, tests to make sure the code is working as expected and this can be
# added to an automation build / test / deployment pipeline

import pytest

from calculate_bmi import cal_bmi
from calculate_bmi import get_category_and_risk
from calculate_bmi import generate_data
from calculate_bmi import verify_count
from calculate_bmi import get_counts_from_ranges
from calculate_bmi import MAPPING

import pandas as pd
import json

@pytest.fixture(scope = 'module')
def df_input():
    json_file_path = 'json_data.json'
    with open(json_file_path, 'r') as fp:
        data = json.load(fp)

    df_input = pd.DataFrame(data)

    return df_input

@pytest.fixture(scope = 'module')
def df_res():
    json_file_path = 'json_data.json'
    with open(json_file_path, 'r') as fp:
        data = json.load(fp)

    df = pd.DataFrame(data)
    df_res = df.apply(generate_data, axis=1)

    return df_res


@pytest.mark.parametrize(
        'heightcm, weight, bmi', [
                            (175, 75, 24.49),
                            (171, 96, 32.83),
                            (167, 82, 29.40),
                            (0, 70, 0),
                            (170, 0, 0)
                            ])
def test_cal_bmi(heightcm, weight, bmi):
    """Assert that BMI is calculate correctly."""

    assert cal_bmi(heightcm, weight) == bmi
    assert cal_bmi(heightcm/100, weight, cm=False) == bmi


@pytest.mark.parametrize(
        'heightm, weight, bmi', [
                            (1.75, 75, 24.49),
                            (1.71, 96, 32.83),
                            (1.67,82, 29.40),
                            (0, 70, 0),
                            (1.70, 0, 0),
                            ])
def test_cal_bmi_in_meters(heightm, weight, bmi):
    """Assert that BMI is calculate correctly when height is passed in meters, in such case set cm=False."""

    assert cal_bmi(heightm, weight, cm=False) == bmi

@pytest.mark.parametrize(
        'heightcm, weight', [
                            (175, 75 ),
                            (171, 96 ),
                            (167, 82 ),
                            (0, 70),
                            (170, 0),
                            ])
def test_cal_bmi_two_decimals(heightcm, weight):
    """Assert that BMI value calculated has at most 2 decimal places"""
    bmi = cal_bmi(heightcm, weight)
    decimals = str(bmi).split(".")[-1]
    assert len(decimals) <=2


@pytest.mark.parametrize(
        'bmi_value, result_category, result_risk', [
                            (24.49, "Normal Weight", 'Low Risk'),
                            (32.83, "Moderately Obese", 'Medium Risk'),
                            (29.40, "Overweight", "Enhanced Risk"),
                            (0, 'Underweight', 'Malnutrition Risk'),
                            ])
def test_get_category_and_risk(bmi_value, result_category, result_risk):
    """Assert that based on BMI value BMI category and Health risk is calculate correctly."""

    assert get_category_and_risk(bmi_value) == (result_category, result_risk)

def test_generate_data_columns(df_input):
    """Assert that for input data frame the correct columns (all three : BMI value, BMI Category, Health risk) are added."""


    df_input = df_input.apply(generate_data, axis=1)

    assert 'BMI value' in df_input.columns
    assert 'BMI Category' in df_input.columns
    assert 'Health risk' in df_input.columns


@pytest.mark.parametrize(
        'input_dict_or_series, result_dict_or_series', [
                            ({"Gender":'Male','HeightCm':175, "WeightKg":75}, {"Gender":'Male','HeightCm':175, "WeightKg":75,
                                                                                'BMI value': 24.49,
                                                                                'BMI Category' : "Normal Weight",
                                                                                'Health risk' : 'Low Risk'}),

                            ({"Gender":'Female','HeightCm':167, "WeightKg":82}, {"Gender":'Female','HeightCm':167, "WeightKg":82,
                                                                                'BMI value': 29.40,
                                                                                'BMI Category' : "Overweight",
                                                                                'Health risk' : 'Enhanced Risk'}),
                            ])
def test_generate_data_values(input_dict_or_series, result_dict_or_series):
    """Assert that for whole data Frame correct values are calculated for all the three columns/fields."""

    result_dict = generate_data(input_dict_or_series)

    assert isinstance(result_dict, dict)
    assert result_dict['BMI value'] == result_dict_or_series['BMI value']
    assert result_dict['BMI Category'] == result_dict_or_series['BMI Category']
    assert result_dict['Health risk'] == result_dict_or_series['Health risk']

@pytest.mark.parametrize(
        'category, result', [
                            ("Normal Weight", 2),
                            ("Overweight", 1),
                            ('unknown', 0)
        ])
def test_get_counts_from_ranges( category, result, request):
    """Assert that number of people calculated from ranges is correct after generating data."""

    df_res = request.getfixturevalue('df_res')

    response = get_counts_from_ranges(df_res, category) 

    assert isinstance(response, int)
    assert response == result
    

@pytest.mark.parametrize(
        'category, counts', [
                            ("Normal Weight", 2),
                            ("Overweight", 1),
                            ('unknown', 0)
        ])
def test_verify_count(category, counts, request):
    """Assert that count of people in different categories are correct by checking with ranges."""

    df_res = request.getfixturevalue('df_res')

    assert verify_count(df_res, category, counts)
