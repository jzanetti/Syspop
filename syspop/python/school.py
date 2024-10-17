
from logging import getLogger


from pandas import DataFrame

from uuid import uuid4

logger = getLogger()

def create_school(school_data: DataFrame, max_student_num: int = 30) -> DataFrame:
    """
    Create a DataFrame of schools filtered by specified areas.

    This function filters the provided school data based on a list of allowed areas,
    then constructs a new DataFrame containing relevant details about each school,
    including its area, minimum and maximum age range, and a unique identifier.

    Parameters:
    ----------
    school_data : DataFrame
        A DataFrame containing school information, including 'area_school',
        'age_min', 'age_max', and 'sector'.

    Returns:
    -------
    DataFrame
        A DataFrame containing the filtered school data with the following columns:
        - area_school: int, the area code of the school
        - age_min: int, the minimum age for students
        - age_max: int, the maximum age for students
        - id: str, a unique identifier for the school
    """
    schools = []

    schools = []
    for _, row in school_data.iterrows():
        area = row["area"]
        age_min = row["age_min"]
        age_max = row["age_max"]
        sector = row["sector"]
        latitude = row["latitude"]
        longitude = row["longitude"]
        max_students = row["max_students"]

        if max_students < max_student_num:
            max_students = max_student_num

        name = f"{sector}_{str(uuid4())[:6]}"
        schools.append({
            "area_school": int(area),
            "age_min": int(age_min),
            "age_max": int(age_max),
            "latitude": float(latitude),
            "longitude": float(longitude),
            "max_students": int(max_students),
            "id": str(name)
        })
    
    return DataFrame(schools)