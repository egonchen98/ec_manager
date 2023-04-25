from calendar import monthrange
import pandas as pd


def generate_enfo_req():
    """Generate requests string to send request"""
    dates = [f'{year}-{month:02}-01/to/{year}-{month:02}-{monthrange(year, month)[1]}' for year in range(2017, 2023) for month in range(1, 13)]  # get all date to be looped
    dates = [f'{year}-{month:02}-01/to/{year}-{month:02}-03' for year in range(2017, 2023) for month in range(1, 13)]  # TODO: detelete this
    param_steps = [['121/122', '0/to/360/by/6'], ['151/165/166/168/169/175/176/177/228', '0/to/360/by/24']]
    area = '31.375/121/31.25/121.125'  # Kunshan

    all_req_kw = []
    for date in dates:
        for param_step in param_steps:
            # [file_id, type, date, param, step, is_num]
            req1 = [f'cf_{date[:10]}_tem.grib', 'cf', '#', date, area] + param_step
            req2 = [f'pf_{date[:10]}_nontem.grib', 'pf', '', date, area] + param_step
            all_req_kw.append(req1)
            all_req_kw.append(req2)

    req_df = pd.DataFrame(all_req_kw, columns=['file_id', 'type', 'number_affix', 'date', 'area', 'param', 'step'])
    req_df['request_time'] = None

    req_df.to_csv('job_manager.csv', index=False)
    return req_df


def main():
    """Main function"""
    generate_enfo_req()


if __name__ == '__main__':
    main()
