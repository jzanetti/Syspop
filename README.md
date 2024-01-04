
# Creating Synthetic Population
A repository for creating synthetic population using census data.

# 1. Required input data:

It is users' responsibilities to create the following required input data from their own census data. One example is given by using the New Zealand Census data in ``etc/scripts_nz/create_nz_data.py``


## 1.1 Population data:

The base population data will be based on the information:
- Number of total people (all ages, ehnicities and genders)
- Number of people (scaled to the number of total people) for different ethnicities _vs_ age
- Number of people (scaled to the number of total people) for different gender _vs_ age

The age must range from 0 to 100. For example:

#### 1.1.1 Number of total people:

| output_area |  0 | 1 | ... | 100 |
| ----------- |  - | - | --- | --- |
| area1       |  30| 50| ... | 10  |
| area2       |  50| 70| ... | 15  |
| ...         | ...|...|...| ... | ... |

#### 1.1.2 Number of people for different ethnicities _vs_ age:

| output_area | ethnicity | 0 | 1 | ... | 100 |
| ----------- | --------- | - | - | --- | --- |
| area1       | Asian     |2.1|1.5| ... | 0.3 |
| area1       | European  |3.0|7.2| ... | 0.1 |
| ...         | ...       |...|...| ... | ... |
| area2       | Asia      |6.0|5.3| ... | 20.0|
| ...         | ...       |...|...| ... | ... |

For example, for a specific area (e.g., ``area1``), the number of people of different ethnicities (e.g., Asian, European, Māori, Pacific Island) who are 15 years old matches the total number of people for that age group (e.g., 15 years old), as detailed in Section 1.1.1.

#### 1.1.3 Number of people for different gender _vs_ age:

| output_area | gender    | 0 | 1 | ... | 100 |
| ----------- | --------- | - | - | --- | --- |
| area1       | female    |1.0|5.0| ... | 0.4 |
| area1       | male      |2.0|12.| ... | 5.5 |
| ...         | ...       |...|...| ... | ... |
| area2       | female    |4.0|19.| ... | 15.0|
| ...         | ...       |...|...| ... | ... |

For example, for a specific area (e.g., ``area1``), the number of people of different genders (e.g., male and female) who are 15 years old matches the total number of people for that age group (e.g., 15 years old), as detailed in Section 1.1.1.

## 2. Household data:
The base population will be assigned to households, the household input data should look like:

| output_area | 0 | 1 | ... | 5   |
| ----------- | - | - | --- | --- |
| area1       | 15| 10| ... | 3   |
| area2       | 10| 20| ... | 11  |
| ...         | ..|...| ... | ... |

where the column names except ``output_area`` indicate the number of dependant children in a household.