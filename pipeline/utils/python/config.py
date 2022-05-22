import configparser


class Config:
    def __init__(self, config_path, config_files):
        self._config = configparser.ConfigParser(allow_no_value=True)
        if config_path is None:
            return
        for config_file in config_files:
            config_file_path = config_path + f'/{config_file}.ini'
            print('Reading configuration from', config_file_path)
            self._config.read(config_file_path)

    def set(self, property_group: str, property_name: str, value: str):
        if not self._config.has_section(property_group):
            self._config.add_section(property_group)
        self._config.set(property_group, property_name, value)

    def get_property_group(self, property_group: str):
        return dict(self._config[property_group])

    def get_property(self, property_group: str, property_name: str):
        return self._config.get(property_group, property_name)