import numpy as np
import pandas as pd


def generate_hhld_complex_microdata(
    seed: int = 2026, n_hhld=500, create_hhld_weight=True
) -> pd.DataFrame:
    """
    Generate a realistic confidential unit-record microdata table with:
      - Hierarchy IDs: household_id, family_id, person_id
      - Continuous features: age, income, saving
      - Categorical features: sex, occupation, location
    """
    rng = np.random.default_rng(seed)

    locations = ["Auckland", "Wellington", "Christchurch", "Hamilton"]
    loc_probs = [0.40, 0.25, 0.20, 0.15]
    loc_income_mult = {
        "Auckland": 1.15,
        "Wellington": 1.18,
        "Christchurch": 1.00,
        "Hamilton": 0.95,
    }

    occupations = [
        "Manager",
        "Professional",
        "Technician",
        "Trade",
        "Clerical",
        "Sales",
    ]
    occ_base_income = {
        "Manager": 95000.0,
        "Professional": 85000.0,
        "Technician": 68000.0,
        "Trade": 64000.0,
        "Clerical": 54000.0,
        "Sales": 48000.0,
    }

    records = []

    # e.g., if generate 120 households (~260 families, ~750 individuals)
    for hh_id in range(1, n_hhld + 1):
        hh_location = rng.choice(locations, p=loc_probs)
        n_families = int(rng.choice([1, 2, 3], p=[0.25, 0.55, 0.20]))

        family_id = 1
        for _ in range(n_families):
            n_members = int(rng.choice([2, 3, 4, 5], p=[0.35, 0.35, 0.20, 0.10]))
            fam_age_center = float(rng.uniform(20.0, 58.0))

            person_id = 1
            for m_idx in range(n_members):
                if m_idx >= 2 and rng.random() < 0.55:
                    # Child / dependent member under 18
                    age = float(np.clip(np.round(rng.uniform(4.0, 17.5), 1), 1.0, 17.9))
                else:
                    age = float(
                        np.clip(
                            np.round(rng.normal(fam_age_center, 11.0), 1), 8.0, 80.0
                        )
                    )
                sex = str(rng.choice(["Male", "Female"], p=[0.49, 0.51]))

                # Occupation probability varies slightly with age
                if age >= 35:
                    occ_p = [0.22, 0.28, 0.16, 0.14, 0.12, 0.08]
                else:
                    occ_p = [0.08, 0.24, 0.20, 0.18, 0.15, 0.15]
                occupation = str(rng.choice(occupations, p=occ_p))

                # Income depends on occupation, location, and age experience
                age_factor = 1.0 + 0.008 * max(0.0, min(age - 22.0, 30.0))
                mean_inc = (
                    occ_base_income[occupation]
                    * loc_income_mult[hh_location]
                    * age_factor
                )
                income = float(max(15000.0, np.round(rng.normal(mean_inc, 9500.0), 2)))

                # Saving correlates with income and age
                saving_rate = float(rng.uniform(0.08, 0.35))
                saving = float(
                    max(
                        0.0,
                        np.round(
                            income * saving_rate * (age / 35.0)
                            + rng.normal(0.0, 3000.0),
                            2,
                        ),
                    )
                )

                records.append(
                    {
                        "household_id": hh_id,
                        "family_id": family_id,
                        "person_id": person_id,
                        "age": age,
                        "sex": sex,
                        "income": income,
                        "saving": saving,
                        "occupation": occupation,
                        "location": hh_location,
                    }
                )
                person_id += 1
            family_id += 1

    raw_df = pd.DataFrame(records)
    if create_hhld_weight:
        rng_w = np.random.default_rng(2026)
        unique_hh = raw_df[["household_id"]].drop_duplicates().reset_index(drop=True)
        unique_hh["household_weight"] = np.round(
            rng_w.uniform(0.8, 3.5, size=len(unique_hh)), 2
        )
        weight_df = unique_hh  # Set to None to run without survey weights
    else:
        weight_df = None

    return (raw_df, weight_df)
