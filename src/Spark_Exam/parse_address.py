def parse_address(address):

    # Split the address by commas
    address_parts = address.split(',')

        # Accessing the elements using indexing 
        
    if len(address_parts) == 4:
        streetNumber = address_parts[0].strip() # strip() - removes any empty spaces around the data"'
        streetName = address_parts[1].strip()
        city = address_parts[2].strip()
        country = address_parts[3].strip()

    # Creating a dictionary with parsed address components
        parsed_address = {
            'streetNumber': streetNumber,
            'streetName': streetName,
            'city': city,
            'country': country
        }
    else:
        # If the address format is not as expected, possibly missing a comma the else statement will print blank entries 
        parsed_address = {
            'streetNumber': '',
            'streetName': '',
            'city': '',
            'country': ''
        }
    # Return the parsed address as a dictionary containing the address fields
    return parsed_address
