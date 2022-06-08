"""
File name: ConfigParser.py
Author: Martin Hein (@heimarti)
Date created: 24/05/2021
Date last modified: 14/06/2021
Version: 1.01
Python Version: 3.9
"""
import configparser


class ConfigParser:
    def __init__(self, **kwargs):
        self._parser = configparser.ConfigParser()
        self._configuration_file_path = kwargs.get('config_file_path')

    def get_config_as_dictionary(self, config_file_path: str):
        """
        The function reads an .ini file, processes it and formats it's contents into a dictionary.
        :param config_file_path: Path to .ini file
        :return: Dictionary object containing .ini file's contents
        """
        try:
            self.parser.read(config_file_path)
            sections = {}
            size_check = self.parser.items()

            if len(size_check) <= 1:
                raise FileNotFoundError

            convert = lambda val: \
                True if (val.lower()).strip() == 'true' else(False if (val.lower()).strip() == 'false' else val)

            for item in self.parser.items():
                section_name = item[0]
                section = dict(self.parser.items(section_name))
                for i, inner_item in section.items():
                    section[i] = convert(inner_item)
                sections[section_name] = section
            return sections
        except FileNotFoundError:
            raise FileNotFoundError('Could not find the specified configuration file.')
        except Exception:
            raise Exception

    @property
    def parser(self):
        return self._parser

    @property
    def configuration_file_path(self):
        return self._configuration_file_path

    @configuration_file_path.setter
    def configuration_file_path(self, value):
        self._configuration_file_path = value









