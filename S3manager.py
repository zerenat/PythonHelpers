import json
import traceback

import requests
import configparser
from typing import Optional


class ConfigParser:
    def __init__(self, **kwargs):
        self._parser = configparser.ConfigParser()
        self._configuration_file_path = kwargs.get('config_file_path') or None

    def get_config_as_dictionary(self, config_file_path: Optional[str] = None):
        """
        The function reads an .ini file, processes it and formats it's contents into a dictionary.
        :param config_file_path: Path to .ini file
        :return: Dictionary object containing .ini file's contents
        """
        if not config_file_path:
            if self._configuration_file_path is not None:
                config_file_path = self._configuration_file_path
            else:
                raise ValueError('Configuration file path undefined.\n'
                                 'Please set by setting value to ConfigParser.configuration_file_path.')
        self.parser.read(config_file_path)
        sections = {}
        size_check = self.parser.items()

        # Check for len bigger than 1 as there will be a default value of 1
        if len(size_check) <= 1:
            raise FileNotFoundError('Could not find the Configuration file. Path invalid.\n'
                                    'Try setting a valid path by using ConfigParser.configuration_file_path.')

        convert = lambda val: \
            True if (val.lower()).strip() == 'true' else(False if (val.lower()).strip() == 'false' else val)

        for item in self.parser.items():
            section_name = item[0]
            section = dict(self.parser.items(section_name))
            for i, inner_item in section.items():
                section[i] = convert(inner_item)
            sections[section_name] = section
        return sections

    @property
    def parser(self):
        return self._parser

    @property
    def configuration_file_path(self):
        return self._configuration_file_path

    @configuration_file_path.setter
    def configuration_file_path(self, value):
        self._configuration_file_path = value


class FileManager:
    def __init__(self):
        self.__supported_types = ["csv", "txt"]

    def read_file(self, file_name: str, file_location: str = ""):
        """
        The function reads a .txt or .csv file from the given file location.
        :param file_name: Name of the file (with extension).
        :param file_location: File location as a path.
        :return: File content.
        """
        type_supported = False
        for supported_type in self.__supported_types:
            if supported_type in file_name:
                type_supported = True
        if not type_supported:
            raise TypeError("File type not supported")
        file_path = "".join((file_location, "\\" if len(file_location) > 0 else "", file_name))
        try:
            with open(file_path, "r") as f:
                in_file = f.readlines()
                f.close()
            in_file = "".join(in_file)
            return in_file
        except FileNotFoundError:
            raise FileNotFoundError(f"Could not find specified {file_name} at "
                                    f"{file_location if len(file_location) > 0 else 'undefined'}\n"
                                    f"Full path: {file_path}")
        except Exception:
            raise Exception(f"An error has occured while attempting to {file_name} from "
                            f"{file_location if len(file_location) > 0 else 'undefined'}\n"
                            f"Trace:\n{traceback.print_exc()}")

    def write_file(self, file_name: str, file_location: str = "", content: str = ""):
        """
        The function writes a .txt or .csv file to the given file location.
        :param file_name: Name of the file (with extension).
        :param file_location: File location as a path.
        :param content: Data to be written to the file.
        :return: Dict object containing the results of the query.
        """
        type_supported = False
        result = "successful"
        for supported_type in self.__supported_types:
            if supported_type in file_name:
                type_supported = True
        if not type_supported:
            raise TypeError("File type not supported")
        file_path = "".join((file_location, "\\" if len(file_location) > 0 else "", file_name))
        try:
            with open(file_path, "w") as f:
                f.write(content)
                f.close()
            return {result: result}
        except FileNotFoundError:
            raise FileNotFoundError(f"Could not find specified {file_name} at "
                                    f"{file_location if len(file_location)>0 else 'undefined'}")
        except Exception:
            raise Exception(f"An error has occured while attempting to {file_name} from "
                            f"{file_location if len(file_location)>0 else 'undefined'}\n"
                            f"Trace:\n{traceback.print_exc()}")


class S3manager:
    def __init__(self, **kwargs):
        self.__auth_token = kwargs.get('auth_token') or None
        self.__api_url = 'https://tqz3wlewoc.execute-api.eu-west-1.amazonaws.com/s3store'

    def __set_up_auth_token(self, token_value: str = None):
        """
        The private function is designed to set up AWS S3 object managing API's access token
        :param token_value: Token value if known. Should be left undefined if not known
        """
        if token_value is not None:
            self.__auth_token = token_value
        else:
            config_file_path = r"\\ant\dept-eu\TBA\UK\Business Analyses\CentralOPS\BIA\AWS_authorizers" \
                               r"\AWS_authorizers.ini"
            try:
                auth_token = ConfigParser().get_config_as_dictionary(config_file_path)['authorizers']['auth-token']
                self.__auth_token = auth_token
            except FileNotFoundError:
                raise FileNotFoundError(f"Could not locate the config file at {config_file_path}")

    def upload_file(self, bucket_name: str, object_name: str, content: str):
        """
        The function makes an object upload to the user chosen S3 bucket.
        :param bucket_name: Destination S3 bucket.
        :param object_name: The name that is given to the content uploaded to S3. Needs to be defined with the file
        extension.
        :param content: The data that is to be stored in the S3 object.
        :return: Dict object containing the results of the query.
        """
        if not self.__auth_token:
            self.__set_up_auth_token()
        response = requests.post(self.__api_url,
                                 headers={'auth-token': self.__auth_token},
                                 data=json.dumps({"bucketName": bucket_name,
                                                  "objectName": object_name,
                                                  "content": content}))
        if response.status_code == 200:
            response = response.json()
            if "errorType" in response:
                return {"result": "fail",
                        "reason": response["errorMessage"]}
            else:
                return {"result": "success"}
        print()

    def download_file(self, bucket_name: str, object_name: str):
        """
        The function downloads an S3 object from a specified S3 bucket.
        :param bucket_name: Name of the destination bucket.
        :param object_name: Name of the S3 object that is to be retreived from the specified S3 bucket.
        :return: Dict object containing the results of the query.
        """
        if not self.__auth_token:
            self.__set_up_auth_token()
        response = requests.get(self.__api_url,
                                headers={'auth-token': self.__auth_token},
                                data=json.dumps({"bucketName": bucket_name,
                                                 "objectName": object_name}))
        if response.status_code == 200:
            response = response.json()
            if "errorType" in response:
                return {"result": "fail",
                        "reason": response["errorMessage"]}
            else:
                return {"result": "success",
                        "data": response["Body"]}


if __name__ == '__main__':

    ##################################################### DEMO 1 #######################################################
    ######################################## Make a S3 bucket upload from a file########################################
    # # 1) Create file manager to read/ write files
    # file_manager = FileManager()
    # # 2) Load file (.txt, .csv currently supported).
    # upload_file = file_manager.read_file(file_name='upload_file.txt', file_location=r"C:\Users\heimarti\Desktop")
    # # 3) Create S3 manager for uploading/ downloading data
    # s3_manager = S3manager()
    # # 4) Upload the file to the S3 bucket of choice. Result is in Dict format.
    # result = s3_manager.upload_file(bucket_name="cobiatestbucket", object_name="upload_file.txt",
    #                                 content=upload_file)
    ################################### Download an object from specified S3 bucket ####################################
    # # 5) Download the chosen file from the specified bucket. Result is in Dict format.
    # download_file = s3_manager.download_file(bucket_name="cobiatestbucket", object_name="upload_file.csv")
    # # 6) Write the data into a file
    # file_manager.write_file(file_name="download_file.txt", content=download_file["data"])
    # # 7) Print the file content
    # print("Content of the S3 object:\n", download_file["data"])
    ####################################################################################################################

    ##################################################### DEMO 2 #######################################################
    ######################################## Make S3 bucket upload from a string########################################
    # # 1) Create S3 manager for uploading/ downloading data
    # s3_manager = S3manager()
    # # 2) Upload the file to the S3 bucket of choice. Result is in Dict format.
    # result = s3_manager.upload_file(bucket_name="cobiatestbucket", object_name="upload_file.txt",
    #                                 content="Hello\n\nThis is a test message!\n\nGood bye.")
    #################################### Download an object from specified S3 bucket ###################################
    # # 3) Download the chosen file from the specified bucket. Result is in Dict format.
    # download_file = s3_manager.download_file(bucket_name="cobiatestbucket", object_name="upload_file.txt")
    # # 4) Print the file content
    # print("Content of the S3 object:\n", download_file["data"])
    ####################################################################################################################
    pass
