from .globals import *

class Task:
    def __init__(self, query: str, response: ResponseType) -> None:
        self.query = query
        self.response = response
