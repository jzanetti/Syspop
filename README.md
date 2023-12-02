
# Creating Synthetic Population
A repository for creating synthetic population using census data.

# Required input data:

## 1. Population data
The base population data will be based on the information:
- Number of people for different ethnicities _vs_ age
- Number of people for different gender _vs_ age

The age must range from 0 to 100. For example:

#### Number of people for different ethnicities _vs_ age

| output_area | ethnicity | 0 | 1 | ... | 100 |
| ----------- | --------- | - | - | --- | --- |
| area1       | Asian     | 30| 50| ... | 10  |
| area2       | European  | 50| 70| ... | 15  |
| ...         | ...       |...|...| ... | ... |

#### Number of people for different gender _vs_ age

| output_area | gender    | 0 | 1 | ... | 100 |
| ----------- | --------- | - | - | --- | --- |
| area1       | female    | 40| 10| ... | 40  |
| area2       | male      | 20| 20| ... | 35  |
| ...         | ...       |...|...| ... | ... |
