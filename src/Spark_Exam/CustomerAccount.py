from dataclasses import dataclass

@dataclass
class CustomerAccount:
    customerId: str
    forename: str
    surname: str
    accounts: list
    streetNumber: int
    streetName: str
    city: str
    country: str