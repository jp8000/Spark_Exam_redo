import pytest

import sys
import os

from src.Spark_Exam.parse_address import parse_address






def test_parse_address():
    address = "123, Test St, Test City, Test Country"
    parsed_address = parse_address(address)
    
    assert parsed_address == {
        'streetNumber': '123',
        'streetName': 'Test St',
        'city': 'Test City',
        'country': 'Test Country'
    }


# when 4 values an not recieved
def test_parse_address_missing_comma():
    address = "123 Test St Test City Test Country"
    parsed_address = parse_address(address)
    
    assert parsed_address == {
        'streetNumber': '',
        'streetName': '',
        'city': '',
        'country': ''
    }



# cd into the "tests" folder 
# command: python3 -m pytest -v test_parse_address.py

