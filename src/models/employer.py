from dataclasses import dataclass


@dataclass(slots=True)
class Employer:
    employer_id: str
    name: str
    url: str
