import time
from string import Template
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import json

import pandas as pd
from ecmwfapi import ECMWFService


def send_request(req: str, target_path: str = 'output.grib'):
    """Send a MARS request to ECMWF with req string"""
    service = ECMWFService("mars")
    target = target_path
    service.execute(req, target)


def get_req(req_dict: dict):
    """Get a MARS request string"""
    req = Template("""
    retrieve,
    class=od,
    expver=1,
        date=$date,
        levtype=sfc,
        $number_kw=$number,
        param=$param,
        step=$step,
        stream=enfo,
        time=00:00:00,
        type=$type,
        area=$area,
        grid=0.25/0.25
    """)

    req1 = req.substitute(req_dict)

    req1 = """
    retrieve,
    class=od,
    date=2018-08-01/to/2018-08-31,
    expver=1,
    levtype=sfc,
    number=1/to/10,
    param=121/122,
    step=6/to/360/by/6,  # start_step is 0 if temperature, else 0
    stream=enfo,
    time=00:00:00,
    type=pf,
    area=31.375/121/31.25/121.125,
    GRID=0.25/0.25,
    target='output'
    """

    return req1


class ReqManager:
    """Manage requests"""
    def __init__(self, data_dir):
        self.jobs = pd.read_csv('./resources/job_manager.csv')
        self.data_dir = data_dir

    def run(self):
        pool = ThreadPoolExecutor(4)
        """Run"""
        for index, row in self.jobs.iterrows():
            file_id = row['file_id']
            target_file = f'{self.data_dir}\\{file_id}'
            if target_file in os.listdir(self.data_dir):
                continue
            req = get_req(row.to_dict())
            pool.submit(send_request, req, target_file)
            time.sleep(2*60)

        pool.shutdown()


def main():
    """Main function"""

    config = json.loads(Path('./resources/config.json').read_text())
    manager = ReqManager(data_dir=config['database'])
    manager.run()


if __name__ == '__main__':
    main()
