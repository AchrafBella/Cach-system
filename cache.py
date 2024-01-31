import hashlib
from time import perf_counter
from datetime import datetime

from PythonPerFHyD.PerfhytCache import connector
from PythonPerFHyD.PerfhytCache.cache_to_protobuf import cache_information_to_protobuf


class Cache:
    def __init__(self, high_threshold=2 << 30, low_threshold=1 << 30, database_path=':memory:'):
        """
        :param high_threshold: (bytes) high threshold
        :param low_threshold: (bytes) low threshold
        :param database_path: the path of database by default is the memory
        """
        # cache configuration
        self.__high_threshold = high_threshold
        self.__low_threshold = low_threshold

        self.__conn = connector.create_connection(database_path)

        from PythonPerFHyD import PythonPerFHyd
        self._perfhyd_version = PythonPerFHyd.__version__
        connector.create_table(self.__conn)

    def get_high_threshold(self):
        return self.__high_threshold

    def get_low_threshold(self):
        return self.__low_threshold

    def get_element_count(self):
        return connector.get_elements_count(self.__conn)

    def get_cache_information(self):
        return connector.get_data_information(self.__conn)

    def get_cache_size(self):
        return connector.get_cache_size(self.__conn)

    def get_cache_size_elements(self):
        return connector.get_size_elements(self.__conn)

    def get_connection(self):
        return self.__conn

    @staticmethod
    def hash(params):
        """ function that hash the parameters
        :param params: the parameters
        :return: the hashcode key
        """
        encoder = hashlib.sha512()
        encoder.update(params)
        return encoder.hexdigest()

    def memoize(self, process, params, deserializer, serializer, current_time=None):
        """ memoize function is the main function in the cache it store data using set and return it using get
        :param process: the process we want to execute
        :param params: the parameters we want to serialize
        :param current_time : the current time this parameters is add for testing
        :param deserializer: deserializer is a function belong to protobuf package that convert serialized object
        :param serializer: serializer function
        to data
        :return the object deserialized using the parameter deserializer
        """

        code_location = process.__code__.co_filename + str(':') + str(process.__code__.co_firstlineno)
        cache_information_pb = cache_information_to_protobuf(str(process.__code__.co_firstlineno),
                                                             process.__code__.co_filename,
                                                             self._perfhyd_version, params.SerializeToString())

        hashcode = Cache.hash(cache_information_pb.SerializeToString())
        data = self.__get(hashcode)

        if data is None:
            start_time = perf_counter()
            value = process(params)
            latency_gain_ms = perf_counter() - start_time

            serialized_value = serializer(value).SerializeToString()

            if current_time is None:
                current_time = datetime.today().timestamp()

            self.__set(hashcode, datetime.today().timestamp(), datetime.today().timestamp(), latency_gain_ms,
                       serialized_value, code_location, self._perfhyd_version, current_time,
                       self.__high_threshold, self.__low_threshold)
            return value
        return deserializer(data[0])

    def __get(self, hashcode):
        """ function that access to the database to get the data
        :param hashcode : hashcode
        :return: return the data
        """
        data = connector.retrieve_data(self.__conn, hashcode)
        if data is not None:
            connector.update_access_time(self.__conn, datetime.today().timestamp(), hashcode)
        return data

    def __set(self, hashcode, insert_time, access_time, latency_gain_ms, value, code_location, perfyt_version,
              current_time, high_threshold, low_threshold):
        """ this set function delete a group of elements based on a threshold, we delete the elements
        with less scores
        :param hashcode: the hashed params
        :param insert_time:
        :param access_time:
        :param latency_gain_ms: the computation duration
        :param value: the data
        :param code_location: location of the code
        :param perfyt_version: the version of programme
        :param high_threshold: (bytes) when we reach to this limit we start release the database
        :param low_threshold: (bytes) we delete data that below this size
        """
        connector.insert_data(self.__conn, hashcode, insert_time, access_time, latency_gain_ms, value, code_location,
                              perfyt_version, current_time, high_threshold, low_threshold)

    def clear(self):
        connector.clear_table(self.__conn)

    def display(self):
        print(*connector.display_data(self.__conn), sep='\n')