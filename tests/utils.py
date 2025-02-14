import random


def random_name(prefix: str) -> str:
    return f"{prefix}{str(random.randint(1000, 9999))}"
