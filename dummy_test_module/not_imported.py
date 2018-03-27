import luigi1


class UnimportedTask(luigi1.Task):
    def complete(self):
        return False
