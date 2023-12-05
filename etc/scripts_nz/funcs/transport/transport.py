
from os.path import join
from pandas import read_csv

from funcs import RAW_DATA
from pickle import dump as pickle_dump


def create_travel(workdir):
    """Write Transport Model file

    Args:
        workdir (str): Working directory
        transport_mode_cfg (dict): Transport model configuration
    """

    data = read_csv(RAW_DATA["transport"]["population_travel_to_work_by_method"])

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
            'Other', 
            'Total']
    ]

    data = data.replace(-999.0, 0)
    data.rename(columns = {
        'SA2_code_usual_residence_address':'output_area_home', 
        'SA2_code_workplace_address':'output_area_work'}, 
        inplace = True)

    with open(join(workdir, "transport.pickle"), 'wb') as fid:
        pickle_dump({
        "transport": data
    }, fid)
