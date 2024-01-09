

# Synthetic & Simulated Population (SysPop)

<p align="center">
    <img src="etc/wiki_img/syspop_wiki.png" alt="Sample Image" width="30%">
</p>

Syspop is developed at [ESR](https://www.esr.cri.nz/home/about-esr/). **See the detailed documentation of SysPop at [SysPop Wiki](https://github.com/jzanetti/Syspop/wiki)**

Contents:

* [Motivation](https://github.com/jzanetti/Syspop#motivations)
* [Attributes](https://github.com/jzanetti/Syspop#attributes)
* [Installation](https://github.com/jzanetti/Syspop#installation)


_Contact: Sijin.Zhang@esr.cri.nz_

## Motivation

This is a repository for creating synthetic population using census data (or other data sources).

Using synthetic populations can be beneficial in various fields and scenarios:

* **Privacy Protection**: Synthetic populations safeguard individual privacy by mimicking real-world patterns without exposing personal details.

* **Simulation and Modeling**: Ideal for simulating scenarios like public health management (e.g., disease spread), social wellbeing studies, urban planning, and traffic management. 
    
    * Synthetic populations enable the study of potential outcomes without using real-world data, which in most cases, is challenging to obtain or impractical.

    * Useful for enhancing datasets, synthetic populations generate additional samples, particularly valuable with limited or incomplete data.

    * Synthetic populations serve as benchmarks for testing and evaluating algorithms and models before application to real-world data.

* **Education and Training**: Synthetic populations provide realistic datasets for educational purposes, allowing students to gain practical experience without accessing sensitive information.

* **Policy and Decision-Making**: Synthetic populations assist policymakers in predicting the impact of different policies and decisions, aiding informed choices based on data-driven insights.

It's important to note that the use of synthetic populations depends on the specific goals of a project and the nature of the data needed. Careful consideration and validation are essential to ensure that synthetic populations accurately reflect the characteristics of the real-world populations they aim to simulate. This package provides a number of interfaces to make creating and validating synthentic population easier.

## Attributes
Attributes can be easily added to the synthetic population. The selection of attributes depends on the available census data. In the case of the New Zealand synthetic population example, the dataset includes over 500 million individuals (agents), with each individual described by:


| Field                       | Description                                                                                           |
|-----------------------------|-------------------------------------------------------------------------------------------------------|
| **Basic:** |                                                                                                       |
| ID                          | Simulated individual/agent ID or name.                                                               |
| Age                         | Age of the simulated agent.                                                                          |
| Gender                      | Gender of the simulated agent.                                                                       |
| Socio-economic Index        | Socio-economic index for each agent.                                                                 |
| Household                   | The household to which the agent belongs, along with the household address.                           |
| Occupation                  | The occupation of the agent (e.g., health sector, education, etc., based on the ANZSCO list), or employment status. |
| **Venue and travel:** |                                                                                                       |
| Company                     | The company where the agent works and its location (e.g., the area where the company is situated).    |
| School                      | The school the agent attends and its location.                                                       |
| Supermarket                 | Supermarket(s) the agent may visit.                                                                  |
| Hospital                    | Hospital(s) the agent may go to in case of illness.                                                  |
| Commute                     | Mode of transportation for the agent (e.g., by bus, train, etc.).                                     |
| **Health Indicators for Each Agent:** |                                                                                                       |
| Self-rated health           |                                                                                                       |
| Life satisfaction           | (e.g., high, moderate, low, etc.).                                                                   |
| Oral health                 | (e.g., good, with removed teeth, all teeth removed).                                                 |
| Mental health               | (e.g., distress level).                                                                             |
| Cardiovascular health       | (e.g., high blood pressure, stroke, heart failure, etc.).                                           |
| Alcohol use                 | (e.g., heavy, moderate, etc.).                                                                      |
| Tobacco use                 | (e.g., heavy, moderate, etc.).                                                                      |
| Vaping use                  | (e.g., heavy, moderate, etc.).                                                                      |
| Illicit drug use            | (e.g., cannabis, cocaine, etc.).                                                                    |
| Nutrition                   | (e.g., fruit eating, veg eating, etc.).                                                             |
| Physical activity           | (e.g., active, etc.).                                                                              |
| Body size                   | (e.g., BMI, etc.).                                                                                 |


## Installation