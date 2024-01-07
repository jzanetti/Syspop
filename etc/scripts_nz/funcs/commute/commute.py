
from os.path import join
from pandas import read_csv

from funcs import RAW_DATA
from pickle import dump as pickle_dump


def create_home_to_work(workdir):
    """Write Transport Model file

    Args:
        workdir (str): Working directory
        transport_mode_cfg (dict): Transport model configuration
    """

    data = read_csv(RAW_DATA["commute"]["workplace_and_home_locations"]["travel-to-work-info"])

    data = data[
        [
            'SA2_code_usual_residence_address',
            'SA2_code_workplace_address',
            'Work_at_home',
            'Drive_a_private_car_truck_or_van', 
            'Drive_a_company_car_truck_or_van',
            'Passenger_in_a_car_truck_van_or_company_bus', 
            'Public_bus', 
            'Train',
            'Bicycle', 
            'Walk_or_jog', 
            'Ferry', 
            'Other']
    ]

    data = data.replace(-999.0, 0)
    data.rename(columns = {
        'SA2_code_usual_residence_address':'area_home', 
        'SA2_code_workplace_address':'area_work'}, 
        inplace = True)

    with open(join(workdir, "commute.pickle"), 'wb') as fid:
        pickle_dump({
        "home_to_work": data
    }, fid)
