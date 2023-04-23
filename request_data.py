from string import Template
from ecmwfapi import ECMWFService


def request(req: str):
    """Send a MARS request to ECMWF with req string"""
    service = ECMWFService("mars")
    target = 'test.grib'
    service.execute(req, target)






