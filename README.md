

# Synthetic & Simulated Population (SysPop)

<p align="center">
    <img src="etc/wiki_img/syspop_wiki.png" alt="Sample Image" width="30%">
</p>

Syspop is developed at [ESR](https://www.esr.cri.nz/home/about-esr/). **See the detailed documentation of SysPop at [SysPop Wiki](https://github.com/jzanetti/Syspop/wiki)**

_Contact: Sijin.Zhang@esr.cri.nz_

## Motivations

This is a repository for creating synthetic population using census data (or other data sources).

Using synthetic populations can be beneficial in various fields and scenarios:

* Privacy Protection: Synthetic populations safeguard individual privacy by mimicking real-world patterns without exposing personal details.

* Simulation and Modeling: Ideal for simulating scenarios like public health management (e.g., disease spread), social wellbeing studies, urban planning, and traffic management. 
    
    * Synthetic populations enable the study of potential outcomes without using real-world data, which in most cases, is challenging to obtain or impractical.

    * Useful for enhancing datasets, synthetic populations generate additional samples, particularly valuable with limited or incomplete data.

    * Synthetic populations serve as benchmarks for testing and evaluating algorithms and models before application to real-world data.

* Education and Training: Synthetic populations provide realistic datasets for educational purposes, allowing students to gain practical experience without accessing sensitive information.

* Policy and Decision-Making: Synthetic populations assist policymakers in predicting the impact of different policies and decisions, aiding informed choices based on data-driven insights.

It's important to note that the use of synthetic populations depends on the specific goals of a project and the nature of the data needed. Careful consideration and validation are essential to ensure that synthetic populations accurately reflect the characteristics of the real-world populations they aim to simulate. This package provides a number of interfaces to make creating and validating synthentic population easier.

## Attributes
Attributes can be easily added in the synthetic population. Which attributes can be added is dependant on the census data that are available. For the example of New Zealand synthetic population, there are 500+ million individuals (agents) included in the dataset, each individual is described by:

- ID: simulated people/agent ID (or name)
- Age: the age of the simulated agent
- Gender: the gender of the simulated agent
- Social-economic index: social economic index for each agent
- Household: which household the agent belongs to, and the address of the household
- Occupation: What is the occupation (e.g., health sector, education etc. based on ANZSCO list) of the agent, or if the agent is employed
- Commute: how the agent travel (e.g., by bus, train etc.)
- Company: which company the agent works for, and where is the company (e.g., which area the company locates)
- School: which school the agent attends, and where is the school
- Supermarket: which supermarket(s) the agent may go
- Hospital: which hospital(s) the agent may go if he/she gets sick
- Health indicators for each agent:
    - self-rated health
    - life satisfaction (e.g., high, moderate, low etc.)
    - oral health (e.g., good, with removed teeth, all teeth removed)
    - mental health (e.g., distress level)
    - cardiovascular health (e.g., high blood pressure, stroke, heart failure etc.)
    - alcohol use (e.g., heavy, moderate etc.)
    - tobacco use (e.g., heavy, moderate etc.)
    - vaping use (e.g., heavy, moderate etc.)
    - illicit drug use (e.g., cannbis, cocaine etc.)
    - nutrition (e.g., fruit eating, veg eating etc.)
    - physical activity (e.g., active etc.)
    - body size (e.g., BMI etc.)

## Installation