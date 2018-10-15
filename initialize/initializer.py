from abc import ABCMeta, abstractmethod
from monitor import HANAServerDBOperatorService
from monitor import HANAServerOSOperatorService
from util import MonitorUtility
from util import MonitorConst as Mc

import traceback


class MonitorInitializer:
    """The root Class for the Initializer
        initialize  --  performing the initialize job
    """
    __metaclass__ = ABCMeta

    def __init__(self):
        self._os_operator = HANAServerOSOperatorService.instance()
        self._db_operator = HANAServerDBOperatorService.instance()

    def get_os_operator(self):
        return self._os_operator

    def get_db_operator(self):
        return self._db_operator

    @abstractmethod
    def initialize(self):
        """abstract method, needs to be overwritten in child classes"""
        pass


class TableInitializer(MonitorInitializer):
    """Initializer for tables, creating all the tables and views, inserting basic records"""
    def __init__(self):
        self.__logger = MonitorUtility.get_logger(Mc.LOGGER_MONITOR_INIT_TABLE)
        super().__init__()

    def initialize(self):
        self._db_operator.execute_from_script(Mc.get_init_sql_file())


class ServerInfoInitializer(MonitorInitializer):
    """Initializer for Server Info, get overview of disk, memory cpu and operation system"""
    def __init__(self):
        self.__logger = MonitorUtility.get_logger(Mc.LOGGER_MONITOR_INIT_SERVER)
        super().__init__()
    
    def __get_server_info(self, ssh, server_id, mount_point):
        """get the overview of disk, memory, CPU and Operation System"""

        self._os_operator.collect_os_info(ssh, server_id, None)
        self._os_operator.collect_disk_info(ssh, server_id, mount_point, None)
        self._os_operator.collect_mem_info(ssh, server_id, None)
        self._os_operator.collect_cpu_info(ssh, server_id, None)

        return self._os_operator.get_server_info_by_server_id(server_id)

    def initialize(self):
        """get the overview of disk, memory, CPU and Operation System for all configured servers"""
        try:
            self.__logger.info("Start initializing server info...")
            for location in self._db_operator.get_locations():
                location_id = location[Mc.FIELD_LOCATION_ID]
                server_name_list = self._db_operator.get_server_full_names(location_id)
                if not server_name_list:
                    self.__logger.error("Server name list is empty, please initialize server name list first!")
                    return
                for server in server_name_list:
                    self.__logger.debug("Processing:{0}".format(server))
                    server_id = server[Mc.FIELD_SERVER_ID]
                    server_name = server[Mc.FIELD_SERVER_FULL_NAME]
                    mount_point = server[Mc.FIELD_MOUNT_POINT]

                    ssh = self._os_operator.open_ssh_connection(server_name)
                    if ssh is None:
                        self.__logger.warning("Failed to connect {0}".format(server_name))
                        continue

                    server_info = self.__get_server_info(ssh, server_id, mount_point)
                    self.__logger.info(server_info)
                    if server_info is not None:
                        self._db_operator.update_server_info(server_info, server_id)
                    ssh.close()
                    self.__logger.info("Initializing {0} is done.".format(server_name))
                self.__logger.info("Successfully initialized server info for location:{0}...".format(location_id))
            self.__logger.info("Successfully initialized server info...")
        except Exception as ex:
            self.__logger.error("Error:{0} happened in initializing server info!".format(ex))
            self.__logger.exception(traceback.format_exc())


class SidInitializer(MonitorInitializer):
    """generate all the SIDs by the configured mapping"""
    def __init__(self):
        self.__logger = MonitorUtility.get_logger(Mc.LOGGER_MONITOR_INIT_SID)
        super().__init__()

    def initialize(self):
        """generate all the SIDs by the configured mapping"""
        try:
            self.__logger.info("Start generating SIDs by the configured mapping...")
            for location in self._db_operator.get_locations():
                location_id = location[Mc.FIELD_LOCATION_ID]
                sid_mappings = self._db_operator.get_sid_mappings()
                if not sid_mappings:
                    self.__logger.error("SID mapping is empty!")
                    return
                server_name_list = self._db_operator.get_server_full_names(location_id)
                if not server_name_list:
                    self.__logger.error("Server name list is empty, please initialize server name list first!")
                    return

                for mapping in sid_mappings:
                    sid_start = mapping[Mc.FIELD_SID_START]
                    sid_end = mapping[Mc.FIELD_SID_END]
                    employee_id = mapping[Mc.FIELD_EMPLOYEE_ID]
                    sid_list = MonitorUtility.gen_sid_list(sid_start, sid_end)
                    if not sid_list:
                        self.__logger.debug(
                            "Failed to generate SIDs for the SID mapping {0}-{1}.".format(sid_start, sid_end))
                        continue
                    # sid_info_list = []
                    # for server in server_name_list:
                    #     # prepare the parameter list for the SQL:
                    #     # insert into VAN_MONITOR.T_SID_INFO (SERVER_ID, SID, SID_USER, EMPLOYEE_ID) values (?,?,?)
                    #     sid_info_list.extend(
                    #         [(server[0], sid, "".join([sid.lower(), "adm"]), employee_id) for sid in sid_list])
                    self._db_operator.insert_sid_info(employee_id, server_name_list, sid_list)
                self.__logger.info("Successfully generated SIDs by the configured "
                                   "mapping for location:{0}...".format(location_id))
            self.__logger.info("Successfully generated SIDs by the configured mapping!")

        except Exception as ex:
            self.__logger.error("Error happened in generating SID info:{0}!".format(ex))
            self.__logger.exception(traceback.format_exc())


class InitController:
    """Facade of the SidInitializer and ServerInfoInitializer"""
    def __init__(self):
        self.table_initializer = TableInitializer()
        self.server_initializer = ServerInfoInitializer()
        self.sid_initializer = SidInitializer()

    def init_server_info(self):
        self.server_initializer.initialize()

    def init_sid_info(self):
        self.sid_initializer.initialize()

    def init_tables(self):
        self.table_initializer.initialize()

    def init_all(self):
        # self.init_tables()
        # self.init_sid_info()
        self.init_server_info()


if __name__ == '__main__':
    InitController().init_all()
    # InitController().init_sid_info()
