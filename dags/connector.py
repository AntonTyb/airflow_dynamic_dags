


class Connector():

    def __init__(self, name):
        self.name = name

    def __call__(self, id):
        return self.run(id)

    def run(self, id):
        print(f"running connector {self.name} for {id}")
        return {"name": self.name, "id":id}

SRC_MAP = {
    "fb.conn1": Connector("fb.conn1"),
    "fb.conn3": Connector("fb.conn3"),
    "gads.conn1": Connector("gads.conn1"),
}