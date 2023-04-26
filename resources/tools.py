from calendar import monthrange
import pandas as pd


def generate_enfo_req():
    """Generate requests string to send request"""
    dates = [f'{year}-01-01/to/{year}-12-31' for year in range(2017, 2023)]  # get all date to be looped
    param_steps = [['121/122', '0/to/360/by/6'], ['151/165/166/168/169/175/176/177/228', '0/to/360/by/24']]
    area = '31.375/121/31.25/121.125'  # Kunshan

    all_req_kw = []
    for date in dates:
        for param_step in param_steps:
            # [file_id, type, date, param, step, is_num]
            pf_numbers = ['1/to/10', '11/to/20', '21/to/30', '31/to/40', '41/to/50']
            req_cf = [f'cf_{date[:10]}_0.grib', 'cf', '#', date, area] + param_step + ['None']
            req_pfs = []
            for i in range(1, 1+len(pf_numbers)):
                req_pfs.append([f'pf_{date[:10]}_{i}.grib', 'pf', '', date, area] + param_step + [pf_numbers[i-1]])

            all_req_kw.extend(req_pfs)
            all_req_kw.append(req_cf)

    req_df = pd.DataFrame(all_req_kw, columns=['file_id', 'type', 'number_affix', 'date', 'area', 'param', 'step', 'number'])
    req_df['request_time'] = None

    req_df.to_csv('job_manager.csv', index=False)
    return req_df


def main():
    """Main function"""
    generate_enfo_req()


if __name__ == '__main__':
    main()
