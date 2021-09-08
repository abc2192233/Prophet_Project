class count:
    def __init__(self):
        self.cpu_info_count = 0
        self.date = None

    def add(self, s):
        if self.date != s:
            self.date = s
            self.cpu_info_count = 0
        elif self.date == s:
            self.cpu_info_count += 1
        return self.cpu_info_count


c = count()


def init_data(s):
    yield s[0], s[1], s[2], c.add(s[0])
