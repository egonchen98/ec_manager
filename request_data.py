from string import Template
from ecmwfapi import ECMWFService


def request(req: str):
    """Send a MARS request to ECMWF with req string"""
    service = ECMWFService("mars")
    target = 'test.grib'
    service.execute(req, target)


def new_func():
    """Test merge scene 2"""
    pass

def get_req():
    """Get a MARS request string"""
    pass


def main():
    pass


if __name__ == '__main__':
    main()

# tewest
# v3
# v4

