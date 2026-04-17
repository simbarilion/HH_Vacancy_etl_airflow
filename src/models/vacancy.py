from dataclasses import dataclass


@dataclass(slots=True)
class Vacancy:
    vac_id: str
    name: str
    url: str
    salary_from: int
    salary_to: int
    area: str
    employer_id: str
