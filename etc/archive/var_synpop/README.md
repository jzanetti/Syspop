
## Why use Variational Optimization approach ? 
Numerous censuses and surveys exist, each originating from diverse temporal and spatial resolutions, and varying in sample size. At present, the Iterative Proportional Fitting (IPF) approach does not perform well ... 

## The example of minimization

The first 50 iterations for creating synthetic population using the Variational Optimization

<p align="center">
    <img src="../../etc/wiki_img/var_examples/loss.gif?raw=true" alt="GIF Example1" width="40%">
    <img src="../../etc/wiki_img/var_examples/census_comparisons.gif?raw=true" alt="Sample Image2" width="40%">
    <img src="../../etc/wiki_img/var_examples/household_composition_survey_comparisons.gif?raw=true" alt="Sample Image3" width="40%">
    <img src="../../etc/wiki_img/var_examples/imms_survey_comparisons.gif?raw=true" alt="Sample Image4" width="40%">
</p>

The following data are integrated using the Variational approach to create the synthetic population:

### Census:
The number of people in different age and ethnicity groups
| Age | Ethnicity | Num |
|-----|-----------|-----|
| 1   | 4         | 2   |
| 1   | 5         | 5   |
| 2   | 4         | 6   |
| 2   | 5         | 6   |
| 3   | 4         | 5   |
| 3   | 5         | 3   |
| 3   | 6         | 5   |
| 4   | 4         | 3   |

### Immunisation Status:
The percentage of people in different immunisation status, groupped by age and ethnicity

| Age | Ethnicity | Imms_status | Num  |
|-----|-----------|-------------|------|
| 1   | 4         | 0           | 0.00 |
| 1   | 4         | 1           | 0.75 |
| 1   | 4         | 2           | 0.25 |
| 3   | 5         | 0           | 0.05 |
| 3   | 5         | 1           | 0.30 |
| 3   | 5         | 2           | 0.65 |

### Household Composition:
The percentage of different types of households
| Age | Depression | Household_composition_code | Num  |
|-----|------------|----------------------------|------|
| 1   | 2          | 12                         | 0.20 |
| 1   | 2          | 13                         | 0.10 |
| 1   | 3          | 11                         | 0.30 |
| 2   | 3          | 11                         | 0.10 |
| 2   | 4          | 11                         | 0.15 |

### Synthetic population created by the above data
| Age | Ethnicity | Immigration Status | Depression | Household Composition Code | Number           |
|-----|-----------|--------------------|------------|----------------------------|------------------|
| 1   | 4         | 0                  | 2          | 12                         | 4.224548e-12     |
| 1   | 4         | 0                  | 2          | 13                         | 3.245442e-12     |
| 1   | 4         | 0                  | 2          | 11                         | 7.067407e-12     |
| 1   | 4         | 0                  | 3          | 12                         | 4.528123e-12     |
| 1   | 4         | 0                  | 3          | 13                         | 4.528123e-12     |
| ... | ...       | ...                | ...        | ...                        | ...              |
| 4   | 6         | 2                  | 3          | 13                         | 1.000000e-02     |
| 4   | 6         | 2                  | 3          | 11                         | 1.000000e-02     |
| 4   | 6         | 2                  | 4          | 12                         | 1.000000e-02     |
| 4   | 6         | 2                  | 4          | 13                         | 1.000000e-02     |
