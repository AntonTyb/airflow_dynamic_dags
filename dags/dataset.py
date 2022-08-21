

fb_ids_a = ["a123","a234","a345"]


def universal_executor(id, name):
    print(f"running universal executor for id: {id} and dataset {name}")
    return f"{name}_{id}"

executor_map = {
    "universal" : universal_executor
}

data_providers_map ={
    "fb.conn1":["i321","i432","i543","i654"],
    "gads.conn1":["g321","g432","g543","g654"],
}
class Dataset():

    def __init__(self, name, connector, executor, ids=None):
        self.name = name # per current naming convention
        self.connector = connector #fb.insights etc
        self.executor_name = executor #universal executor or custom
        self.executor = executor_map[executor]
        self.ids = ids #NativeID aka Native_ID

    def get_providers(self):
        if self.ids is not None:
            return self.ids
        else:
            return data_providers_map[self.connector]
Datasets = [
    Dataset("fb1","fb.conn1","universal"),
    Dataset("fb3","fb.conn3","universal", ids = fb_ids_a),
    Dataset("gads3","gads.conn1","universal")
]

