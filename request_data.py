from string import Template
from ecmwfapi import ECMWFService


def request(req: str):
    """Send a MARS request to ECMWF with req string"""
    service = ECMWFService("mars")
    target = 'test.grib'
    service.execute(req, target)


def get_req():
    """Get a MARS request string"""
    pass

def main():
    pass


if __name__ == '__main__':
    main()



