import re


class Version:
    def __init__(self, version):
        version = version.replace('alpha', '0') \
            .replace('beta', '1') \
            .replace('rc', '2') \
            .replace('sr', '3') \
            .replace('a', '.0') \
            .replace('b', '.1')

        version_list = list(map(int, re.split('-|\\.', version)))
        self.version_list = version_list

    def __lt__(self, other):
        _prepare_versions(self.version_list, other.version_list)
        for i in range(0, len(self.version_list)):
            if self.version_list[i] != other.version_list[i]:
                if self.version_list[i] < other.version_list[i]:
                    return True
                else:
                    return False
        return False

    def __le__(self, other):
        _prepare_versions(self.version_list, other.version_list)
        for i in range(0, len(self.version_list)):
            if self.version_list[i] != other.version_list[i]:
                if self.version_list[i] < other.version_list[i]:
                    return True
                else:
                    return False
        return True

    def __gt__(self, other):
        _prepare_versions(self.version_list, other.version_list)
        for i in range(0, len(self.version_list)):
            if self.version_list[i] != other.version_list[i]:
                if self.version_list[i] > other.version_list[i]:
                    return True
                else:
                    return False
        return False

    def __ge__(self, other):
        _prepare_versions(self.version_list, other.version_list)
        for i in range(0, len(self.version_list)):
            if self.version_list[i] != other.version_list[i]:
                if self.version_list[i] > other.version_list[i]:
                    return True
                else:
                    return False
        return True

    def __eq__(self, other):
        _prepare_versions(self.version_list, other.version_list)
        for i in range(0, len(self.version_list)):
            if self.version_list[i] != other.version_list[i]:
                return False
        return True

    def __ne__(self, other):
        _prepare_versions(self.version_list, other.version_list)
        for i in range(0, len(self.version_list)):
            if self.version_list[i] != other.version_list[i]:
                return True
        return False


def _prepare_versions(list_1, list_2):
    while len(list_1) < len(list_2):
        list_1.append(0)
    while len(list_2) < len(list_1):
        list_2.append(0)


def main():
    to_test = [
        ('1.0.0', '2.0.0'),
        ('1.0.0', '1.42.0'),
        ('1.2.0', '1.2.42'),
        ('1.1.0-alpha', '1.2.0-alpha.1'),
        ('1.0.1b', '1.0.10-alpha.beta'),
        ('1.0.0-rc.1', '1.0.0'),
        ('1.1.3', '2.2.3'),
        ('1.3.0', '0.3.0'),
        ('0.3.0b', '1.2.42'),
        ('1.3.42', '42.3.1')
    ]

    for version_1, version_2 in to_test:
        print(Version(version_1) < Version(version_2), f'{version_1} < {version_2}')
    print()
    for version_1, version_2 in to_test:
        print(Version(version_1) <= Version(version_2), f'{version_1} <= {version_2}')
    print()
    for version_1, version_2 in to_test:
        print(Version(version_1) == Version(version_2), f'{version_1} == {version_2}')
    print()
    for version_1, version_2 in to_test:
        print(Version(version_1) != Version(version_2), f'{version_1} != {version_2}')
    print()
    for version_1, version_2 in to_test:
        print(Version(version_1) >= Version(version_2), f'{version_1} >= {version_2}')
    print()
    for version_1, version_2 in to_test:
        print(Version(version_1) > Version(version_2), f'{version_1} > {version_2}')


if __name__ == "__main__":
    main()
