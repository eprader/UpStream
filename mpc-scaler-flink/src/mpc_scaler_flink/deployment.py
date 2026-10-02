class Deployment:
    def __init__(self, name: str, number_of_taskslots: int, replica_count: int):
        if replica_count < 0:
            raise ValueError(
                f"Expected a positive integer for `replica count`, got {replica_count}"
            )
        if number_of_taskslots <= 0:
            raise ValueError(
                f"Expected a positive integer for `number_of_taskslots`, got {number_of_taskslots}"
            )
        self.replica_count = replica_count
        self.number_of_taskslots = number_of_taskslots
        self.name = name

    def __repr__(self):
        return f"Deployment(name={self.name}, taskslots={self.number_of_taskslots}, replica_count={self.replica_count})"
