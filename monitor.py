from operation.db_operations import HANAMonitorDAO
from operation.os_operations import SUSEMonitorDAO
from operation.os_operations import RedHatMonitorDAO
from operation.simulate_os_operator import LinuxMonitorOAOSimulator as OSSimulator
from util import MonitorUtility as Mu
from util import MonitorConst as Mc
from errors import MonitorDBOpError
from errors import MonitorOSOpError
from errors import MonitorDBError
from errors import MonitorError
from abc import ABC, abstractmethod

from datetime import datetime
from operator import itemgetter

import threading
import time
import re


class Monitor(ABC):
    """The root (abstract) Class for the Monitor Tools
        accept      --  Accept the updater to perform the DB operations
        monitoring  --  performing the monitoring job for memory, CPU and Disk
    """

    def __init__(self):
        self._os_operator = HANAServerOSOperatorService.instance()
        self._db_operator = HANAServerDBOperatorService.instance()

    def accept(self, updater):
        """Accept the visitors to perform the DB operations"""
        updater.update(self)

    def get_os_operator(self):
        return self._os_operator

    def get_db_operator(self):
        return self._db_operator

    def get_locations(self):
        return self._db_operator.get_locations()

    def get_check_interval(self):
        return Mc.get_db_check_interval(self._db_operator)

    @abstractmethod
    def monitoring(self, check_id, location_id):
        """abstract method, needs to be overwritten in child classes"""
        pass


class MemoryMonitor(Monitor):
    """Monitoring for Memory, get top 5 memory consumers relative to HANA by user from all the servers"""

    def __init__(self):
        self.__logger = Mu.get_logger(Mc.LOGGER_MONITOR_MEM)
        super().__init__()

    def monitoring(self, check_id, location_id):
        Mu.log_debug(self.__logger, "[{0}]Memory Monitoring begin...".format(check_id), location_id)
        # initialize the working hours (just for loading values from database)
        working_time = Mc.get_db_operation_time(self._db_operator)
        Mu.log_debug(self.__logger, "Working hours is {0}".format(working_time), location_id)
        server_name_list = self._db_operator.get_server_full_names(location_id)
        if not server_name_list:
            # As Memory Monitor is the first process, try to reconnect DB if server list is empty
            # Will not do it in the following processes (to make sure the consistency)
            Mu.log_info(self.__logger, "Server list is empty, reconnect and have a try again...", location_id)
            try:
                self._db_operator.refresh_monitor_dao()
                server_name_list = self._db_operator.get_server_full_names(location_id)
            except MonitorDBError as ex:
                Mu.log_warning(self.__logger,
                               "Exception happened in refresh_monitor_dao(), error:{0}".format(ex),
                               location_id)

        if server_name_list:
            # As Memory Monitor is the first process, set catalog status to "IN PROCESS"
            self.accept(MonitorCatalogUpdater(check_id,
                                              location_id,
                                              Mc.MONITOR_STATUS_SERVER_ID_OVER_ALL,
                                              Mc.MONITOR_STATUS_IN_PROCESS))

        for server in server_name_list:
            Mu.log_debug(self.__logger, "Processing:{0}".format(server), location_id)
            server_id = server[Mc.FIELD_SERVER_ID]
            server_name = server[Mc.FIELD_SERVER_FULL_NAME]
            server_os = server[Mc.FIELD_OS]

            # Reset server info at the beginning, does not need it in cpu and disk monitoring process.
            # for fixing bug#37, If some server died during the process, this script would reuse previous value
            # Modified at 07/28/2018
            self._os_operator.reset_server_info(server_id)

            self.accept(MonitorCatalogUpdater(check_id,
                                              location_id,
                                              server_id,
                                              Mc.MONITOR_STATUS_IN_PROCESS,
                                              server_name))
            Mu.log_info(self.__logger, "Trying to connect {0}".format(server_name), location_id)
            ssh = self._os_operator.open_ssh_connection(server_name)
            if ssh is None:
                Mu.log_warning(self.__logger, "Failed to connect {0}".format(server_name), location_id)

                self.accept(MonitorCatalogUpdater(check_id,
                                                  location_id,
                                                  server_id,
                                                  Mc.MONITOR_STATUS_ERROR,
                                                  "Failed to connect {0}".format(server_name)))

                # Update M_SERVER_INFO (with empty info) as well even
                # failed to connect the server  ** updated at 07/21/2018 **
                # self._db_operator.update_server_monitoring_info(
                #     check_id,
                #     self._os_operator.get_server_info_by_server_id(server_id),
                #     server_id)
                # Update at 07/03/2019 use visitor instead of updating monitor table directly
                self.accept(MonitorResourceUpdater(check_id,
                                                   server_id,
                                                   self._os_operator.get_server_info_by_server_id(server_id),
                                                   None))  # set it to none to skip update_mem_monitoring_info
                continue
            try:
                Mu.log_info(self.__logger, "Connected {0}".format(server_name), location_id)
                Mu.log_debug(self.__logger, "Trying to get memory overview of {0}".format(server_name), location_id)
                # collect memory info for one server by server id
                self._os_operator.collect_mem_info(ssh, server_id, server_os)
                # get total memory and free memory from the "memory info" which collected by last step
                mem_total, mem_free = self._os_operator.get_server_info(server_id, Mc.SERVER_INFO_MEM)
                Mu.log_debug(self.__logger,
                             "Memory overview of {0} is (total:{1}, free:{2})".format(server_name, mem_total, mem_free),
                             location_id)

                mem_consumers = self._os_operator.get_mem_consumers(ssh, server_id, server_os)
                Mu.log_debug(self.__logger,
                             "memory consuming information for {0}:{1}".format(server_name, mem_consumers),
                             location_id)

                # commented it because we need to save overall info even the detail is empty -- 07/21/2018
                # the empty checking logic moved to dbOperator's update_mem_monitoring_info
                # if top_5_mem_consumers:
                server_info = self._os_operator.get_server_info_by_server_id(server_id)
                if server_info is not None:
                    self.accept(MonitorResourceUpdater(check_id,
                                                       server_id,
                                                       server_info,
                                                       mem_consumers))
            finally:
                self._os_operator.close_ssh_connection(ssh)

            Mu.log_info(self.__logger, "Memory Monitoring is done for {0}.".format(server_name), location_id)

            self.accept(MonitorCatalogUpdater(check_id,
                                              location_id,
                                              server_id,
                                              Mc.MONITOR_STATUS_COMPLETE,
                                              "Memory Monitoring is done for {0}.".format(server_name)))
        Mu.log_debug(self.__logger, "Memory Monitoring finished.", location_id)


class CPUMonitor(Monitor):
    """Monitoring for CPU, get top 5 CPU consumers that relative to HANA by user from all the servers"""

    def __init__(self):
        self.__logger = Mu.get_logger(Mc.LOGGER_MONITOR_CPU)
        super().__init__()

    def monitoring(self, check_id, location_id):
        Mu.log_debug(self.__logger, "[{0}]CPU Monitoring begin...".format(check_id), location_id)
        server_name_list = self._db_operator.get_server_full_names(location_id)

        for server in server_name_list:
            Mu.log_debug(self.__logger, "Processing:{0}".format(server), location_id)
            server_id = server[Mc.FIELD_SERVER_ID]
            server_name = server[Mc.FIELD_SERVER_FULL_NAME]
            server_os = server[Mc.FIELD_OS]

            self.accept(MonitorCatalogUpdater(check_id,
                                              location_id,
                                              server_id,
                                              Mc.MONITOR_STATUS_IN_PROCESS,
                                              server_name))

            Mu.log_info(self.__logger, "Trying to connect {0}".format(server_name), location_id)
            ssh = self._os_operator.open_ssh_connection(server_name)
            if ssh is None:
                Mu.log_warning(self.__logger, "Failed to connect {0}".format(server_name), location_id)

                self.accept(MonitorCatalogUpdater(check_id,
                                                  location_id,
                                                  server_id,
                                                  Mc.MONITOR_STATUS_ERROR,
                                                  "Failed to connect {0}".format(server_name)))
                # Update M_SERVER_INFO (with empty info) as well even
                # failed to connect the server  ** updated at 07/21/2018 **
                # self._db_operator.update_server_monitoring_info(
                #     check_id,
                #     self._os_operator.get_server_info_by_server_id(server_id),
                #     server_id)
                # Update at 07/03/2019 use visitor instead of updating monitor table directly
                self.accept(MonitorResourceUpdater(check_id,
                                                   server_id,
                                                   self._os_operator.get_server_info_by_server_id(server_id),
                                                   None))  # set it to none to skip update_mem_monitoring_info
                continue
            try:
                Mu.log_info(self.__logger, "Connected {0}".format(server_name), location_id)
                Mu.log_debug(self.__logger, "Trying to get CPU overview of {0}".format(server_name), location_id)
                # collect cpu info for one server by server id
                self._os_operator.collect_cpu_info(ssh, server_id, server_os)
                # get cpu number and cpu utilization from the "cpu info" which collected by last step via server id
                cpu_num, cpu_usage = self._os_operator.get_server_info(server_id, Mc.SERVER_INFO_CPU)
                Mu.log_debug(self.__logger,
                             "CPU overview of {0} is (num:{1}, usage:{2})".format(server_name, cpu_num, cpu_usage),
                             location_id)

                cpu_consumers = self._os_operator.get_cpu_consumers(ssh, server_id, server_os)
                Mu.log_debug(self.__logger,
                             "CPU consuming information for {0}:{1}".format(server_name, cpu_consumers),
                             location_id)

                # commented it because we need to save overall info even the detail is empty -- 07/21/2018
                # the empty checking logic moved to dbOperator's update_cpu_monitoring_info
                # if top_5_cpu_consumers:
                server_info = self._os_operator.get_server_info_by_server_id(server_id)
                if server_info is not None:
                    self.accept(MonitorResourceUpdater(check_id,
                                                       server_id,
                                                       server_info,
                                                       cpu_consumers))
            finally:
                self._os_operator.close_ssh_connection(ssh)
            Mu.log_info(self.__logger, "CPU Monitoring is done for {0}.".format(server_name), location_id)

            self.accept(MonitorCatalogUpdater(check_id,
                                              location_id,
                                              server_id,
                                              Mc.MONITOR_STATUS_COMPLETE,
                                              "CPU Monitoring is done for {0}.".format(server_name)))
        Mu.log_debug(self.__logger, "CPU Monitoring finished.", location_id)


class DiskMonitor(Monitor):
    """Monitoring for Disk, get disk consuming information by user from all the servers"""

    def __init__(self):
        self.__logger = Mu.get_logger(Mc.LOGGER_MONITOR_DISK)
        super().__init__()

    def monitoring(self, check_id, location_id):
        Mu.log_debug(self.__logger, "[{0}]Disk Monitoring begin...".format(check_id), location_id)
        server_name_list = self._db_operator.get_server_full_names(location_id)

        for server in server_name_list:
            Mu.log_debug(self.__logger, "Processing:{0}".format(server), location_id)

            server_id = server[Mc.FIELD_SERVER_ID]
            server_name = server[Mc.FIELD_SERVER_FULL_NAME]
            mount_point = server[Mc.FIELD_MOUNT_POINT]
            server_os = server[Mc.FIELD_OS]

            self.accept(MonitorCatalogUpdater(check_id,
                                              location_id,
                                              server_id,
                                              Mc.MONITOR_STATUS_IN_PROCESS,
                                              server_name))
            Mu.log_info(self.__logger, "Trying to connect {0}".format(server_name), location_id)
            ssh = self._os_operator.open_ssh_connection(server_name)
            if ssh is None:
                Mu.log_warning(self.__logger, "Failed to connect {0}".format(server_name), location_id)

                self.accept(MonitorCatalogUpdater(check_id,
                                                  location_id,
                                                  server_id,
                                                  Mc.MONITOR_STATUS_ERROR,
                                                  "Failed to connect {0}".format(server_name)))
                # Update M_SERVER_INFO (with empty info) as well even
                # failed to connect the server  ** updated at 07/21/2018 **
                # self._db_operator.update_server_monitoring_info(
                #     check_id,
                #     self._os_operator.get_server_info_by_server_id(server_id),
                #     server_id)
                # Update at 07/03/2019 use visitor instead of updating monitor table directly
                self.accept(MonitorResourceUpdater(check_id,
                                                   server_id,
                                                   self._os_operator.get_server_info_by_server_id(server_id),
                                                   None))  # set it to none to skip update_mem_monitoring_info
                continue
            try:
                Mu.log_info(self.__logger, "Connected {0}".format(server_name), location_id)
                Mu.log_debug(self.__logger, "Trying to get disk overview of {0}".format(server_name), location_id)
                # collect disk overview info for one server by server id
                self._os_operator.collect_disk_info(ssh, server_id, mount_point, server_os)
                # get total disk size and free disk size from the "disk info" which collected by last step via server id
                disk_total, disk_free = self._os_operator.get_server_info(server_id, Mc.SERVER_INFO_DISK)
                Mu.log_debug(self.__logger,
                             "Disk overview of {0} is (total:{1}, free:{2})".format(server_name, disk_total, disk_free),
                             location_id)

                is_sudo_need_password = self._db_operator.is_sudo_need_password(server_id)
                disk_consumers = self._os_operator.get_disk_consumers(ssh,
                                                                      server_id,
                                                                      mount_point,
                                                                      server_os,
                                                                      Mc.is_user_sudoer(),
                                                                      is_sudo_need_password)

                Mu.log_debug(self.__logger,
                             "Disk consuming information for {0}:{1}".format(server_name, disk_consumers),
                             location_id)

                # commented it because we need to save overall info even the detail is empty -- 07/21/2018
                # the empty checking logic moved to dbOperator's update_cpu_monitoring_info
                # if consuming_info:
                server_info = self._os_operator.get_server_info_by_server_id(server_id)
                if server_info is not None:
                    self.accept(MonitorResourceUpdater(check_id,
                                                       server_id,
                                                       server_info,
                                                       disk_consumers))
            finally:
                self._os_operator.close_ssh_connection(ssh)
            Mu.log_info(self.__logger, "Disk Monitoring is done for {0}.".format(server_name), location_id)

            self.accept(MonitorCatalogUpdater(check_id,
                                              location_id,
                                              server_id,
                                              Mc.MONITOR_STATUS_COMPLETE,
                                              "Disk Monitoring is done for {0}.".format(server_name)))
        # set over all status to complete
        self.accept(MonitorCatalogUpdater(check_id,
                                          location_id,
                                          Mc.MONITOR_STATUS_SERVER_ID_OVER_ALL,
                                          Mc.MONITOR_STATUS_COMPLETE))
        Mu.log_debug(self.__logger, "Disk Monitoring finished.", location_id)


class InstanceInfoMonitor(Monitor):
    """Collecting basic info for hana instances, update hana instance info from all the servers.
    Including SID, instance number, host, server name, revision, edition and so on.
    Not to affect the overall status calculation, this stage is not counted when calculating status of overall stage
    """

    def __init__(self):
        self.__logger = Mu.get_logger(Mc.LOGGER_MONITOR_INSTANCE)
        super().__init__()

    def monitoring(self, check_id, location_id):
        Mu.log_debug(self.__logger, "[{0}]Instance Monitoring begin...".format(check_id), location_id)
        server_name_list = self._db_operator.get_server_full_names(location_id)

        for server in server_name_list:
            Mu.log_debug(self.__logger, "Processing:{0}".format(server), location_id)
            server_id = server[Mc.FIELD_SERVER_ID]
            server_name = server[Mc.FIELD_SERVER_FULL_NAME]
            mount_point = server[Mc.FIELD_MOUNT_POINT]
            server_os = server[Mc.FIELD_OS]

            # stage of collecting instance info will not affect the overall stage, not like the other three stages
            self.accept(MonitorCatalogUpdater(check_id,
                                              location_id,
                                              server_id,
                                              Mc.MONITOR_STATUS_IN_PROCESS,
                                              server_name))
            Mu.log_info(self.__logger, "Trying to connect {0}".format(server_name), location_id)
            ssh = self._os_operator.open_ssh_connection(server_name)
            if ssh is None:
                Mu.log_warning(self.__logger, "Failed to connect {0}".format(server_name), location_id)

                self.accept(MonitorCatalogUpdater(check_id,
                                                  location_id,
                                                  server_id,
                                                  Mc.MONITOR_STATUS_ERROR,
                                                  "Failed to connect {0}".format(server_name)))
                continue
            try:
                Mu.log_info(self.__logger, "Connected {0}".format(server_name), location_id)
                Mu.log_debug(self.__logger, "Trying to get instance info of {0}".format(server_name), location_id)
                # collect instance info for one server by server id
                instance_info = self._os_operator.get_all_hana_instance_info(ssh, server_id, server_os)
                Mu.log_debug(self.__logger,
                             "Instance information of {0} is {1}".format(server_name, instance_info),
                             location_id)

                if instance_info:  # will skip update database if instance info is empty
                    self.accept(MonitorResourceUpdater(check_id,
                                                       server_id,
                                                       None,
                                                       instance_info))
                else:
                    Mu.log_debug(self.__logger,
                                 "Instance information for {0} is empty, skipped updating db.".format(server_name))
            finally:
                self._os_operator.close_ssh_connection(ssh)
            Mu.log_info(self.__logger, "Instance Monitoring is done for {0}.".format(server_name), location_id)

            self.accept(MonitorCatalogUpdater(check_id,
                                              location_id,
                                              server_id,
                                              Mc.MONITOR_STATUS_COMPLETE,
                                              "Instance Monitoring is done for {0}.".format(server_name)))

        Mu.log_debug(self.__logger, "Instance Monitoring finished.", location_id)


class DBUpdater(ABC):
    @abstractmethod
    def update(self, monitor):
        """abstract method, needs to be overwritten in child classes"""
        pass


class MonitorResourceUpdater(DBUpdater):
    """Visitor for all monitor classes, it's for updating information of the monitoring processes including
    the monitoring for the memory, CPU and disk to HANA DB"""

    def __init__(self, check_id, server_id, server_info, monitor_updates):
        self.__check_id = check_id
        self.__server_id = server_id
        self.__server_info = server_info
        self.__monitor_updates = monitor_updates

    def update(self, monitor):
        if isinstance(monitor, MemoryMonitor):
            self.__update_mem_monitor(monitor)
        elif isinstance(monitor, CPUMonitor):
            self.__update_cpu_monitor(monitor)
        elif isinstance(monitor, DiskMonitor):
            self.__update_disk_monitor(monitor)
        elif isinstance(monitor, InstanceInfoMonitor):
            self.__update_instance_info_monitor(monitor)

    def __update_mem_monitor(self, monitor):
        # update the memory monitoring info with:
        # 1. the server memory overview info (self.server_info)
        # 2. top 5 memory consuming info (self.monitor_updates).
        monitor.get_db_operator().update_mem_monitoring_info(self.__check_id,
                                                             self.__server_id,
                                                             self.__server_info,
                                                             self.__monitor_updates)

    def __update_cpu_monitor(self, monitor):
        # update the cpu monitoring info with:
        # 1. the server cpu overview info (self.server_info)
        # 2. top 5 cpu consuming info (self.monitor_updates).
        monitor.get_db_operator().update_cpu_monitoring_info(self.__check_id,
                                                             self.__server_id,
                                                             self.__server_info,
                                                             self.__monitor_updates)

    def __update_disk_monitor(self, monitor):
        # update the disk monitoring info with:
        # 1. the server disk overview info (self.server_info)
        # 2. top 5 disk consuming info (self.monitor_updates).
        monitor.get_db_operator().update_disk_monitoring_info(self.__check_id,
                                                              self.__server_id,
                                                              self.__server_info,
                                                              self.__monitor_updates)

    def __update_instance_info_monitor(self, monitor):
        # update instance info with self.monitor_updates
        monitor.get_db_operator().update_instance_info(self.__check_id,
                                                       self.__server_id,
                                                       self.__monitor_updates)


class MonitorCatalogUpdater(DBUpdater):
    """Visitor for monitor catalog from the monitor classes , it's for updating status of the monitoring processes to
    the monitoring catalog to HANA DB"""

    def __init__(self, check_id, location_id, server_id=None, status=None, msg=None):
        self.__check_id = check_id
        self.__server_id = server_id
        self.__status = status
        self.__location_id = location_id
        self.__msg = msg
        self.__stage = None

    def update(self, monitor):
        if isinstance(monitor, MemoryMonitor):
            # over all status can only be recorded from memory monitor
            self.__stage = Mc.MONITOR_STAGE_OVERALL \
                if self.__server_id == Mc.MONITOR_STATUS_SERVER_ID_OVER_ALL else Mc.MONITOR_STAGE_MEM
        elif isinstance(monitor, CPUMonitor):
            self.__stage = Mc.MONITOR_STAGE_CPU
        elif isinstance(monitor, DiskMonitor):
            # over all status can only be recorded in disk monitor
            if self.__server_id == Mc.MONITOR_STATUS_SERVER_ID_OVER_ALL and self.__status == Mc.MONITOR_STATUS_COMPLETE:
                self.__update_monitor_catalog_overall_finish(monitor)
                return
            else:
                self.__stage = Mc.MONITOR_STAGE_DISK
        elif isinstance(monitor, InstanceInfoMonitor):
            self.__stage = Mc.MONITOR_STAGE_INSTANCE
        else:
            raise MonitorError("Unknown class: {0}!".format(monitor.__class__.__name__))

        self.__update_monitor_catalog(monitor)

    def __update_monitor_catalog(self, monitor):
        monitor.get_db_operator().update_monitor_catalog(self.__check_id,
                                                         self.__server_id,
                                                         self.__stage,
                                                         self.__status,
                                                         self.__location_id,
                                                         self.__msg)

    def __update_monitor_catalog_overall_finish(self, monitor):
        """set monitor catalog overall stage to FINISH status, the status might be ERROR, WARNING or COMPLETE"""
        monitor.get_db_operator().update_monitor_catalog_overall_status(self.__check_id, self.__location_id)


class MonitorExtension(Monitor):
    """Root class to provide the improvements of some features for current monitor"""

    def __init__(self, monitor):
        super().__init__()
        self._monitor = monitor

    def monitoring(self, check_id, location_id):
        self._monitor.monitoring(check_id, location_id)


class MonitorAdminExtension(MonitorExtension):
    """Decorator of monitor for sending monitor mails to administrators:
    1. memory: when total available memory less then threshold:
        a. will send email to administrator
        b. will send email to the top 5 persons who consume most memory
        c. and/or will shutdown the relative instance automatically if the memory less then the threshold for
        three (continuously) times.
    2. cpu: when total available cpu less then threshold
        a. will send email to administrator
        b. will send email to the top 5 person who consume most CPU
    3. disk: when total available disk less then threshold
        a. will send email to administrator
        b. will send email to the top 5 person who consume most disk
    """

    def __init__(self, monitor):
        super().__init__(monitor)
        self.__logger = Mu.get_logger(Mc.LOGGER_MONITOR_EXTENSION)

    def monitoring(self, check_id, location_id):
        super().monitoring(check_id, location_id)
        # Get current server overview info (including the CPU, Disk, Memory)
        current_process_status = self._monitor.get_db_operator().get_current_process_status(location_id)

        if current_process_status != Mc.MONITOR_STATUS_COMPLETE and \
                current_process_status != Mc.MONITOR_STATUS_WARNING and \
                current_process_status != Mc.MONITOR_STATUS_ERROR:
            # do not use current_process_status == Mc.MONITOR_STATUS_IN_PROCESS
            # because there might be some other in process status be added.
            # self._logger.debug("Do not have complete server info for location {0}, waiting for next round... "
            #                    "Current process status is {1}".format(location_id, current_process_status))
            Mu.log_debug(self.__logger,
                         "Do not have complete server info, waiting for next round... "
                         "Current process status is {0}".format(current_process_status),
                         location_id)
            return

        current_servers_info = self._monitor.get_db_operator().get_current_servers_info(location_id)

        for server_info in current_servers_info:
            server_id = server_info[Mc.FIELD_SERVER_ID]
            server_name = server_info[Mc.FIELD_SERVER_FULL_NAME]
            Mu.log_debug(self.__logger, "Checking threshold for ({0}:{1})".format(server_id, server_name))

            # check memory, if free memory less than threshold, send the email out
            # and fix it (shutdown one HANA which consuming the largest memory (after three warnings)
            memory_free = server_info[Mc.FIELD_MEM_FREE]
            memory_total = server_info[Mc.FIELD_MEM_TOTAL]
            if memory_free is not None and memory_free >= 0 and memory_total is not None and memory_total > 0:
                self.__check_memory_notify_and_fix(server_info, location_id)
            # check disk, if free disk less than threshold, send the email out
            disk_free = server_info[Mc.FIELD_DISK_FREE]
            disk_total = server_info[Mc.FIELD_DISK_TOTAL]
            if disk_free is not None and disk_free >= 0 and disk_total is not None and disk_total > 0:
                self.__check_disk_notify(server_info, location_id)
            # check cpu, if cpu utilization higher than threshold, send the email out
            cpu_usage = server_info[Mc.FIELD_CPU_UTILIZATION]
            if cpu_usage is not None and cpu_usage >= 0:
                self.__check_cpu_notify(server_info, location_id)

        # check the monitoring status, if the process failed on the server for all the three stages, send the email out
        self.__check_monitoring_status_and_email(check_id, location_id)
        Mu.log_info(self.__logger, "Check id: {0} is completed for improver.".format(check_id), location_id)

    def __check_memory_notify_and_fix(self, server_info, location_id):
        """check the memory for the provided server.
        If current memory is less than the predefined threshold, will send the warning email to the top 5
        memory consumers.
        If the memory less then the threshold for three times continuously, will try to shutdown the HANA instance
        which consuming the most memory.
        """
        server_id = server_info[Mc.FIELD_SERVER_ID]
        server_name = server_info[Mc.FIELD_SERVER_FULL_NAME]
        memory_free = server_info[Mc.FIELD_MEM_FREE]
        memory_total = server_info[Mc.FIELD_MEM_TOTAL]
        os = server_info[Mc.FIELD_OS]

        free_mem_threshold = ((100 - Mc.get_db_mem_usage_warn_threshold(self._monitor.get_db_operator(),
                                                                        self.__logger)) * memory_total) / 100

        Mu.log_debug(self.__logger,
                     "Server:{0}, free Memory:{1}, threshold:{2}".format(server_name, memory_free, free_mem_threshold),
                     location_id)
        # send email out if free memory less then the threshold
        if memory_free is not None and memory_free < free_mem_threshold:
            # sending warning email to top 5 memory consumers
            top5_mem_consumers = self._monitor.get_db_operator().get_top5_memory_consumers(server_id)
            Mu.log_debug(self.__logger,
                         "Server ({0}), top 5 memory consumers:{1}".format(server_name, top5_mem_consumers),
                         location_id)

            # If it's not working time, skip following part (Sending email and performing HDB stop)
            if not Mu.is_current_time_working_time():
                Mu.log_info(self.__logger, "Skip checking sending email "
                                           "(and performing HDB stop) because of the non-working time.")
                return

            email_to = [consumer[Mc.FIELD_EMAIL]
                        for consumer in top5_mem_consumers if consumer[Mc.FIELD_EMAIL] is not None]
            Mu.log_debug(self.__logger, "[MEM] Sending email to:{0}".format(email_to), location_id)
            Mu.send_email(Mc.get_db_email_sender(self._monitor.get_db_operator(), self.__logger),
                          email_to,
                          "[MONITOR.MEM] {0} is Running Out of Memory".format(server_name),
                          Mu.generate_email_body(server_info, Mc.SERVER_INFO_MEM, top5_mem_consumers),
                          self._monitor.get_db_operator().get_email_admin(location_id))

            # performing HDB stop for the highest memory consumer
            # if the condition meets (continues checking failed for three times)
            last3_servers_info = self._monitor.get_db_operator().get_last3_servers_info(server_id,
                                                                                        free_mem_threshold,
                                                                                        Mc.SERVER_INFO_MEM)
            failure_times = len(last3_servers_info) if last3_servers_info else 0
            Mu.log_debug(self.__logger, "Continuous checking failure time(s):{0}".format(failure_times), location_id)
            if failure_times >= Mc.get_db_max_failure_times(self._monitor.get_db_operator(), self.__logger):
                # get the highest consumer (the sid with filter flag will be skipped) and perform HDB stop
                highest_consumer = self.__get_highest_memory_consumer(top5_mem_consumers, server_id, location_id)
                if highest_consumer:
                    user_name = highest_consumer[Mc.FIELD_USER_NAME]
                    sid = Mu.get_sid_from_sidadm(user_name)

                    mem_usage = highest_consumer[Mc.FIELD_USAGE]
                    server_name = highest_consumer[Mc.FIELD_SERVER_FULL_NAME]
                    email = highest_consumer[Mc.FIELD_EMAIL]
                    employee_name = highest_consumer[Mc.FIELD_EMPLOYEE_NAME]
                    Mu.log_info(self.__logger,
                                "Server ({0}), highest memory consumer:({1}, {2})".format(server_name, sid, mem_usage),
                                location_id)

                    # get ssh with <sid>user and the default password
                    Mu.log_info(self.__logger, "Trying to connect {0}".format(server_name), location_id)
                    ssh = self._monitor.get_os_operator().open_ssh_connection(server_name,
                                                                              user_name,
                                                                              Mc.get_sidadmin_default_password())
                    if ssh is None:
                        Mu.log_warning(self.__logger,
                                       "Failed to connect {0}, with user {1}".format(server_name, user_name),
                                       location_id)
                    else:
                        try:
                            Mu.log_info(self.__logger,
                                        "Try to shutdown HANA for {0} on {1}, because server "
                                        "is running out of memory and {2} is consuming highest "
                                        "({3}%) memory.".format(sid, server_name, user_name, mem_usage),
                                        location_id)

                            self._monitor.get_os_operator().shutdown_hana(ssh, os)
                            Mu.log_info(self.__logger,
                                        "HANA:{0} on {1} shutdown is processed.".format(sid, server_name),
                                        location_id)
                            if email is not None and len(email) > 0:
                                # sending email to the owner of the instance
                                email_to = [email]
                                email_body = ("Dear {0}, \n\n{1} is running out of memory, your {2} is "
                                              "shutting down because it's consuming highest memory. "
                                              "If this SID is very important and you do not want "
                                              "it to be shut down next time, please contact administrator"
                                              " to mark it as an important SID. \n -- this is only a testing email "
                                              "your hana will not be shut down really, please do it manually."
                                              "\n\nRegards,\nHANA OS "
                                              "Monitor".format(employee_name, server_name, sid))
                                Mu.log_debug(self.__logger, "[MEM] Sending email to:{0} for "
                                                            "shutting down HANA.".format(email_to), location_id)
                                Mu.send_email(Mc.get_db_email_sender(self._monitor.get_db_operator(), self.__logger),
                                              email_to,
                                              "[MONITOR.MEM] {0} on {1} is Shutting Down".format(sid, server_name),
                                              email_body,
                                              self._monitor.get_db_operator().get_email_admin(location_id))
                            else:
                                Mu.log_info(self.__logger,
                                            "HANA:{0} on {1} shutdown is processed, "
                                            "but no email configured.".format(sid, server_name),
                                            location_id)
                        finally:
                            self._monitor.get_os_operator().close_ssh_connection(ssh)

    def __get_highest_memory_consumer(self, top5_mem_consumers, server_id, location_id):
        # get the consumer which consuming highest memory, skip the important server
        # highest_consumer = max(top5_mem_consumers, key=lambda x: x["USAGE"])
        if not top5_mem_consumers:
            return None
        for i in range(0, len(top5_mem_consumers)):
            sid = Mu.get_sid_from_sidadm(top5_mem_consumers[i]["USER_NAME"])
            if self._monitor.get_db_operator().is_important_server(sid, server_id):
                Mu.log_debug(self.__logger,
                             "skip the important SID:{0} in server (id):{1}".format(sid, server_id),
                             location_id)
                continue
            return top5_mem_consumers[i]

    def __check_disk_notify(self, server_info, location_id):
        """check the disk for the provided server.
        If current disk is less than the predefined threshold, will send the warning email to the top 5
        disk consumers.
        """
        server_id = server_info[Mc.FIELD_SERVER_ID]
        server_name = server_info[Mc.FIELD_SERVER_FULL_NAME]
        disk_free = server_info[Mc.FIELD_DISK_FREE]
        disk_total = server_info[Mc.FIELD_DISK_TOTAL]

        free_disk_threshold = ((100 - Mc.get_db_disk_usage_warn_threshold(self._monitor.get_db_operator(),
                                                                          self.__logger)) * disk_total) / 100

        Mu.log_debug(self.__logger,
                     "Server:{0}, free disk:{1}, threshold:{2}".format(server_name, disk_free, free_disk_threshold),
                     location_id)
        if disk_free is not None and disk_free < free_disk_threshold:
            # sending warning email to top 5 disk consumers
            top5_disk_consumers = self._monitor.get_db_operator().get_top5_disk_consumers(server_id)
            Mu.log_debug(self.__logger,
                         "Server ({0}), top 5 disk consumers:{1}".format(server_name, top5_disk_consumers),
                         location_id)

            # If it's not working time, skip following part (Sending email)
            if not Mu.is_current_time_working_time():
                Mu.log_info(self.__logger, "Skip sending email because of the non-working time.")
                return

            email_to = [consumer["EMAIL"] for consumer in top5_disk_consumers if consumer["EMAIL"] is not None]
            Mu.log_debug(self.__logger, "[DISK] Sending email to:{0}".format(email_to), location_id)
            Mu.send_email(Mc.get_db_email_sender(self._monitor.get_db_operator(), self.__logger),
                          email_to,
                          "[MONITOR.DISK] {0} is Running Out of Disk".format(server_name),
                          Mu.generate_email_body(server_info, Mc.SERVER_INFO_DISK, top5_disk_consumers),
                          self._monitor.get_db_operator().get_email_admin(location_id))

    def __check_monitoring_status_and_email(self, check_id, location_id):
        """check whether there are some servers which all the three stages monitoring process are failed,
        and send mail to administrators to warn this."""
        servers = self._monitor.get_db_operator().get_failed_servers(check_id, location_id)
        if not servers:
            return

        # If it's not working time, skip following part (Sending email)
        if not Mu.is_current_time_working_time():
            Mu.log_info(self.__logger, "Skip sending email for failed server(s) {0} "
                                       "because of the non-working time.".format(servers))
            return

        subject = "[MONITOR.TASKS] failed on {0} servers".format(len(servers)) \
            if len(servers) > 1 else "[MONITOR.TASKS] failed on 1 server"
        body = "Monitoring process failed on:"
        for server in servers:
            body = "".join([body, "\n\t", server])
        body = "".join([body,
                        "\n",
                        "Normally the monitoring process failed because of the connection not working, "
                        "please have a check with the relative connection(s)."])
        Mu.send_email(Mc.get_db_email_sender(self._monitor.get_db_operator(), self.__logger),
                      self._monitor.get_db_operator().get_email_admin(location_id),
                      subject,
                      body)

    def __check_cpu_notify(self, server_info, location_id):
        """check the cpu for the provided server.
        If current cpu utilization is higher than the predefined threshold, will send the warning email to the top 5
        cpu consumers.
        """
        server_id = server_info[Mc.FIELD_SERVER_ID]
        server_name = server_info[Mc.FIELD_SERVER_FULL_NAME]
        cpu_usage = server_info[Mc.FIELD_CPU_UTILIZATION]

        cpu_threshold = Mc.get_db_cpu_usage_warn_threshold(self._monitor.get_db_operator(), self.__logger)

        Mu.log_debug(self.__logger,
                     "Server:{0}, cpu usage:{1}, threshold:{2}".format(server_name, cpu_usage, cpu_threshold),
                     location_id)
        if cpu_usage is not None and cpu_usage > cpu_threshold:
            # sending warning email to top 5 CPU consumers
            top5_cpu_consumers = self._monitor.get_db_operator().get_top5_cpu_consumers(server_id)
            Mu.log_debug(self.__logger,
                         "Server ({0}), top 5 cpu consumers:{1}".format(server_name, top5_cpu_consumers),
                         location_id)

            # If it's not working time, skip following part (Sending email)
            if not Mu.is_current_time_working_time():
                Mu.log_info(self.__logger, "Skip sending email because of the non-working time.")
                return

            email_to = [consumer["EMAIL"] for consumer in top5_cpu_consumers if consumer["EMAIL"] is not None]
            Mu.log_debug(self.__logger, "[CPU] Sending email to:{0}".format(email_to), location_id)
            Mu.send_email(Mc.get_db_email_sender(self._monitor.get_db_operator(), self.__logger),
                          email_to,
                          "[MONITOR.CPU] {0} is Running Out of CPU Resource".format(server_name),
                          Mu.generate_email_body(server_info, Mc.SERVER_INFO_CPU, top5_cpu_consumers),
                          self._monitor.get_db_operator().get_email_admin(location_id))


class HANAServerOSOperatorService:
    """ Server OS side operator, responsible for all shell command operations, it's designed as singleton.
    To get the instance of this class: HANAServerOSOperatorService.instance()
    Initialize the class using HANAServerOSOperatorService() will raise an exception.
    """
    __instance = None

    @staticmethod
    def instance():
        """static access method for singleton"""
        if HANAServerOSOperatorService.__instance is None:
            HANAServerOSOperatorService()
        return HANAServerOSOperatorService.__instance

    def __init__(self):
        if HANAServerOSOperatorService.__instance is not None:
            raise MonitorOSOpError("This class is a singleton, use HANAServerOSOperatorService.instance() instead")
        else:
            HANAServerOSOperatorService.__instance = self
            self.__suse_dao = SUSEMonitorDAO()
            self.__redhat_dao = RedHatMonitorDAO()
            self.__server_info = {}
            # base64.b64decode(Mc.SSH_DEFAULT_PASSWORD).decode("utf-8")
            self.__os_passwd = Mu.get_decrypt_string(Mc.get_rsa_key_file(), Mc.get_ssh_default_password())
            self.__os_user = Mc.get_ssh_default_user()
            self.__logger = Mu.get_logger(Mc.LOGGER_MONITOR_SERVER_OS_OPERATOR)

    def __get_dao(self, server_os=None):
        if server_os is None or len(server_os) == 0:
            Mu.log_debug(self.__logger, "The relative server does not have 'OS' information, using default value.")
            server_os = Mc.get_ssh_default_os_type()
            # raise MonitorOSOpError("The relative server does not have 'OS' information, failed at '__get_dao'")

        return self.__suse_dao if "SUSE" in server_os.upper() else self.__redhat_dao

    def open_ssh_connection(self, server_name, user_name=None, user_password=None):
        if user_name is None or user_password is None:
            user_name, user_password = self.__os_user, self.__os_passwd

        Mu.log_debug(self.__logger, "Trying to connect {0}.".format(server_name))
        ssh = self.__get_dao().open_ssh_connection(server_name, user_name, user_password)
        if ssh is not None:
            Mu.log_debug(self.__logger, "Connected {0}.".format(server_name))
        return ssh

    def close_ssh_connection(self, ssh):
        self.__get_dao().close_ssh_connection(ssh)

    def __init_server_info_dict(self, server_id):
        self.__server_info[server_id] = {Mc.FIELD_DISK_TOTAL: None,
                                         Mc.FIELD_DISK_FREE: None,
                                         Mc.FIELD_MEM_TOTAL: None,
                                         Mc.FIELD_MEM_FREE: None,
                                         Mc.FIELD_CPU_NUMBER: None,
                                         Mc.FIELD_CPU_UTILIZATION: None,
                                         Mc.FIELD_OS: None}

    def __set_server_info(self, server_id, info_type, *args):
        if len(args) < 2:
            Mu.log_error(self.__logger, "Error in __set_server_info, number of arguments < 2")
            return
        if server_id not in self.__server_info:
            self.__init_server_info_dict(server_id)

        if info_type == Mc.SERVER_INFO_MEM:
            self.__server_info[server_id][Mc.FIELD_MEM_TOTAL] = args[0]
            self.__server_info[server_id][Mc.FIELD_MEM_FREE] = args[1]
        elif info_type == Mc.SERVER_INFO_CPU:
            self.__server_info[server_id][Mc.FIELD_CPU_NUMBER] = args[0]
            self.__server_info[server_id][Mc.FIELD_CPU_UTILIZATION] = args[1]
        elif info_type == Mc.SERVER_INFO_DISK:
            self.__server_info[server_id][Mc.FIELD_DISK_TOTAL] = args[0]
            self.__server_info[server_id][Mc.FIELD_DISK_FREE] = args[1]
        elif info_type == Mc.SERVER_INFO_OS:
            self.__server_info[server_id][Mc.FIELD_OS] = args[0]
            self.__server_info[server_id][Mc.FIELD_KERNEL] = args[1]

    def reset_server_info(self, server_id):
        """reset the __server_info to empty value"""
        if server_id in self.__server_info:
            self.__init_server_info_dict(server_id)

    def collect_disk_info(self, ssh, server_id, mount_point, os):
        """collect disk info, including total size and unused size"""
        if Mc.use_simulator_4_disk():
            # use simulator is USE_SIMULATOR is True
            total_size, unused_size = OSSimulator.simulate_collect_disk_info()
        else:
            os_output = self.__get_dao(os).collect_disk_info(ssh, mount_point)
            if os_output is None:
                Mu.log_warning(self.__logger, "Can not get disk info for server:{0}, "
                                              "mount_point:{1}.".format(server_id, mount_point))
                total_size = -1
                unused_size = -1
            else:
                try:
                    results = os_output[0].split()
                    total_size = float(results[0])
                    unused_size = float(results[1])
                except Exception as ex:
                    total_size = -1
                    unused_size = -1
                    Mu.log_warning(self.__logger, "Parsing SSH output failed in 'collect_disk_info' with error: {0}, "
                                                  "server: {1}, the output: {2}".format(ex, server_id, os_output))

        self.__set_server_info(server_id, Mc.SERVER_INFO_DISK, total_size, unused_size)

    def collect_mem_info(self, ssh, server_id, os):
        """ get the overall memory information for system"""
        if Mc.use_simulator_4_mem():
            # use simulator if USE_SIMULATOR is True
            mem_total, mem_free = OSSimulator.simulate_collect_mem_info()
        else:
            os_output = self.__get_dao(os).collect_mem_info(ssh)

            if os_output is None:
                Mu.log_warning(self.__logger, "Can not get memory info for server:{0}.".format(server_id))
                mem_total = -1
                mem_free = -1
            else:
                try:
                    results = os_output[0].split()
                    mem_total = int(results[0])
                    mem_free = int(results[1])
                except Exception as ex:
                    mem_total = -1
                    mem_free = -1
                    Mu.log_warning(self.__logger, "Parsing SSH output failed in 'collect_mem_info' with error: {0}, "
                                                  "server: {1}, the output: {2}".format(ex, server_id, os_output))

        self.__set_server_info(server_id, Mc.SERVER_INFO_MEM, mem_total, mem_free)

    def collect_cpu_info(self, ssh, server_id, os):
        """ get the overall CPU information for system"""
        if Mc.use_simulator_4_cpu():
            # use simulator if USE_SIMULATOR is True
            cpu_number, cpu_usage = OSSimulator.simulate_collect_cpu_info()
        else:
            os_output_cpu_number, os_output_cpu_usage = self.__get_dao(os).collect_cpu_info(ssh)
            # get cpu number
            if os_output_cpu_number is None:
                Mu.log_warning(self.__logger, "Can not get cpu number info for server:{0}.".format(server_id))
                cpu_number = -1
            else:
                try:
                    cpu_number = int(os_output_cpu_number[0])
                except Exception as ex:
                    cpu_number = -1
                    Mu.log_warning(self.__logger, "Parsing SSH output failed in 'collect_cpu_info(0)' "
                                                  "with error: {0}, server: {1}, "
                                                  "the output: {2}".format(ex, server_id, os_output_cpu_number))
            # get cpu usage
            if os_output_cpu_usage is None:
                Mu.log_warning(self.__logger, "Can not get cpu usage info for server:{0}.".format(server_id))
                cpu_usage = -1
            else:
                try:
                    cpu_usage = float(os_output_cpu_usage[0])
                except Exception as ex:
                    cpu_usage = -1
                    Mu.log_warning(self.__logger, "Parsing SSH output failed in 'collect_cpu_info(1)' "
                                                  "with error: {0}, server: {1}, "
                                                  "the output: {2}".format(ex, server_id, os_output_cpu_usage))

        self.__set_server_info(server_id, Mc.SERVER_INFO_CPU, cpu_number, cpu_usage)

    def collect_os_info(self, ssh, server_id, os):
        """get os info, including os version and kernel version"""
        os_output_os_version, os_output_os_kernel = self.__get_dao(os).collect_os_info(ssh)
        # get os version
        if os_output_os_version is None:
            Mu.log_warning(self.__logger, "Can not OS release info for server:{0}, ".format(server_id))
            os_version = ''
        else:
            try:
                os_version = str(os_output_os_version[0]) \
                    .split('=')[1].replace('\\n', '').replace('"', '').replace("'", "")
            except Exception as ex:
                os_version = ''
                Mu.log_warning(self.__logger, "Parsing SSH output failed in 'collect_os_info(OS)' "
                                              "with error: {0}, server: {1}, "
                                              "the output: {2}".format(ex, server_id, os_output_os_version))

        # get kernel version
        if os_output_os_kernel is None:
            Mu.log_warning(self.__logger, "Can get not OS kernel info for server:{0}, ".format(server_id))
            os_kernel = ''
        else:
            try:
                os_kernel = str(os_output_os_kernel[0]).replace('\\n', '')
            except Exception as ex:
                os_kernel = ''
                Mu.log_warning(self.__logger, "Parsing SSH output failed in 'collect_os_info(Kernel)' with error: {0}, "
                                              "server: {1}, the output: {2}".format(ex, server_id, os_output_os_kernel))

        self.__set_server_info(server_id, Mc.SERVER_INFO_OS, os_version, os_kernel)

    def get_mem_consumers(self, ssh, server_id, os):
        """Get memory consumers for all users, contains top 5 memory consumers for every user """
        if Mc.use_simulator_4_mem():
            mem_consumers = OSSimulator.simulate_get_mem_consumers(ssh)
        else:
            os_output = self.__get_dao(os).get_mem_consumers(ssh)

            if os_output is None:
                Mu.log_warning(self.__logger, "Can not get memory consumers of ({0}).".format(server_id))
                mem_consumers = []
            else:
                try:
                    # replace ' <defunct>' to '<defunct>' to prevent from using <defunct> as process id
                    mem_consumers = [i.replace(' <defunct>', '<defunct>').split() for i in os_output]
                    mem_consumers = [{Mc.FIELD_USER_NAME: i[0],
                                      Mc.FIELD_PROCESS_COMMAND: i[1],
                                      Mc.FIELD_PROCESS_ID: i[2],
                                      Mc.FIELD_MEM: i[3]} for i in mem_consumers if len(i) > 3]

                except Exception as ex:
                    mem_consumers = []
                    Mu.log_warning(self.__logger, "Parsing SSH output failed in 'get_mem_consumers' with error:{0}, "
                                                  "server:{1}, the output:{2}".format(ex, server_id, os_output))
        return mem_consumers

    def get_cpu_consumers(self, ssh, server_id, os):
        """Get cpu consumers for all users, contains top 5 cpu consumers for every user """
        if Mc.use_simulator_4_cpu():
            # use simulator if USE_SIMULATOR is True
            cpu_consumers = OSSimulator.simulate_get_cpu_consumers(ssh)
        else:
            os_output = self.__get_dao(os).get_cpu_consumers(ssh)

            if os_output is None:
                Mu.log_warning(self.__logger, "Can not get cpu consumers for ({0}).".format(server_id))
                cpu_consumers = []
            else:
                try:
                    cpu_consumers = [{Mc.FIELD_USER_NAME: i.split()[1],
                                      Mc.FIELD_PROCESS_COMMAND: i.split()[11],
                                      Mc.FIELD_PROCESS_ID: i.split()[0],
                                      Mc.FIELD_CPU: i.split()[8]} for i in os_output if len(i.split()) > 11]

                    # In some system, some user might have the same user id (by the wrong setting),
                    # which lead to the duplicate key issue.
                    # remove duplicated records (list(set()) will not work, because CPU utilization may not the same)
                    cpu_consumers_clean = []
                    for consumer in cpu_consumers:
                        duplicate_flag = False
                        for consumer_clean in cpu_consumers_clean:
                            if consumer[Mc.FIELD_USER_NAME] == consumer_clean[Mc.FIELD_USER_NAME] and \
                                    consumer[Mc.FIELD_PROCESS_COMMAND] == consumer_clean[Mc.FIELD_PROCESS_COMMAND] and \
                                    consumer[Mc.FIELD_PROCESS_ID] == consumer_clean[Mc.FIELD_PROCESS_ID]:
                                duplicate_flag = True
                                break
                        if duplicate_flag:
                            continue
                        else:
                            cpu_consumers_clean.append(consumer)

                    cpu_consumers = cpu_consumers_clean
                except Exception as ex:
                    cpu_consumers = []
                    Mu.log_warning(self.__logger, "Parsing SSH output failed in 'get_cpu_consumers' with error:{0}, "
                                                  "server:{1}, the output:{2}".format(ex, server_id, os_output))

        return cpu_consumers

    def get_disk_consumers(self, ssh, server_id, mount_point, os, is_sudosudoers=False, neeed_sudo_pwd=False):
        """ Get the disk consuming information for mount_point (default value is /usr/sap)"""
        # if USE_SIMULATOR is True, will get the data from simulator instead of OS
        if Mc.use_simulator_4_disk():
            disk_total, disk_free = self.get_server_info(server_id, Mc.SERVER_INFO_DISK)
            disk_usage_info = OSSimulator.simulate_get_disk_consumers(ssh, disk_total, disk_free)
        else:
            os_output = self.__get_dao(os).get_disk_consumers(ssh,
                                                              mount_point,
                                                              is_sudosudoers,
                                                              neeed_sudo_pwd,
                                                              self.__os_passwd)
            os_output_owners = []
            if os_output is None:
                Mu.log_warning(self.__logger, "Can not get disk consumers for "
                                              "({0}:{1}).".format(server_id, mount_point))
                disk_usage_info = []
            else:
                try:
                    # get owner of the all folders in mount_point
                    os_output_owners = self.__get_dao(os).get_owners_of_sub_folders(ssh, mount_point)

                    # for filter purpose, add "/" at the end of mount_point
                    mount_point = "".join([mount_point, "/"])

                    disk_usage_info = [{Mc.FIELD_DISK_USAGE_KB: i.split()[0],
                                        Mc.FIELD_FOLDER: i.split()[1]
                                        [i.split()[1].startswith(mount_point) and len(mount_point):],
                                        Mc.FIELD_USER_NAME: next(
                                            (j.split()[0] for j in os_output_owners
                                             if len(j.split()) == 2 and i.split()[1] == j.split()[1]), '')}
                                       for i in os_output if len(i.split()) == 2 and mount_point in i.split()[1]]
                    # disk_usage_info = \
                    #     [{Mc.FIELD_DISK_USAGE_KB: i.split()[0],
                    #       Mc.FIELD_SID: i.split()[1][i.split()[1].startswith(mount_point) and len(mount_point):]}
                    #      for i in cmd_output if len(i.split()) == 2 and mount_point in i.split()[1]]

                except Exception as ex:
                    disk_usage_info = []
                    Mu.log_warning(self.__logger, "Parsing SSH output failed in 'get_disk_consumers' with error: {0}, "
                                                  "server: {1}, the output: {2}, "
                                                  "owners: {3}".format(ex, server_id, os_output, os_output_owners))
        return disk_usage_info

    def get_all_sid_users(self, ssh, server_id, os):
        """get all sid users (sidadm) """
        os_output = self.__get_dao(os).get_all_sid_users(ssh)

        if os_output is None:
            Mu.log_warning(self.__logger, "Can not get all sid users for server:{0}.".format(server_id))
            sid_users = []
        else:
            try:
                sid_users = [i.replace('\n', '') for i in os_output]
            except Exception as ex:
                sid_users = []
                Mu.log_warning(self.__logger, "Parsing SSH output failed in 'get_all_sid_users' with error: {0}, "
                                              "server: {1}, the output: {2}".format(ex, server_id, os_output))
        return sid_users

    def get_all_hana_instance_info(self, ssh, server_id, os, path=None):
        """get instance info for all hana instance"""
        os_output = self.__get_dao(os).get_all_hana_instance_info(ssh, path)
        if os_output is None:
            Mu.log_warning(self.__logger, "Can not get hana instance info for server:{0}.".format(server_id))
            hana_info = []
        else:
            try:
                i = 0
                hana_info = []
                while i < len(os_output):
                    if "HDB_ALONE" in os_output[i]:
                        info = {Mc.FIELD_SID: os_output[i].split(" ")[0]}
                        i += 1
                        while i < len(os_output) and "HDB_ALONE" not in os_output[i]:
                            if re.match("HDB[0-9][0-9]", os_output[i].strip()):
                                info[Mc.FIELD_INSTANCE_NO] = os_output[i].strip()[3:].strip()
                            elif re.match("hosts?", os_output[i].strip()):
                                info[Mc.FIELD_HOST] = "{0} [{1}]".format(
                                    len(os_output[i].split(":")[1].strip().split(","))
                                    if len(os_output[i].split(":")[1].strip()) > 0 else 0,
                                    os_output[i].split(":")[1].strip())
                            elif "version:" in os_output[i]:
                                info[Mc.FIELD_REVISION] = os_output[i].split(" ")[1].strip()
                            elif "edition:" in os_output[i]:
                                info[Mc.FIELD_EDITION] = os_output[i].split(":")[1].strip()

                            i += 1
                        hana_info.append(info)

            except Exception as ex:
                hana_info = []
                Mu.log_warning(self.__logger, "Parsing SSH output failed in 'get_all_hana_instance_info' with error:"
                                              " {0}, server: {1}, the output: {2}".format(ex, server_id, os_output))
        return hana_info

    def get_server_info(self, server_id, info_type):
        """ get the server information via following info_types:
        MonitorConst.SERVER_INFO_MEM
        MonitorConst.SERVER_INFO_CPU
        MonitorConst.SERVER_INFO_DISK
        MonitorConst.SERVER_INFO_OS
        """
        if server_id in self.__server_info:
            return {
                Mc.SERVER_INFO_MEM: (self.__server_info[server_id][Mc.FIELD_MEM_TOTAL],
                                     self.__server_info[server_id][Mc.FIELD_MEM_FREE]),
                Mc.SERVER_INFO_CPU: (self.__server_info[server_id][Mc.FIELD_CPU_NUMBER],
                                     self.__server_info[server_id][Mc.FIELD_CPU_UTILIZATION]),
                Mc.SERVER_INFO_DISK: (self.__server_info[server_id][Mc.FIELD_DISK_TOTAL],
                                      self.__server_info[server_id][Mc.FIELD_DISK_FREE]),
                Mc.SERVER_INFO_OS: (self.__server_info[server_id][Mc.FIELD_OS]),
            }.get(info_type, None)

    def get_server_info_by_server_id(self, server_id):
        if server_id not in self.__server_info:
            return {Mc.FIELD_MEM_TOTAL: None,
                    Mc.FIELD_MEM_FREE: None,
                    Mc.FIELD_CPU_NUMBER: None,
                    Mc.FIELD_CPU_UTILIZATION: None,
                    Mc.FIELD_DISK_TOTAL: None,
                    Mc.FIELD_DISK_FREE: None,
                    Mc.FIELD_OS: None,
                    Mc.FIELD_KERNEL: None}

        return self.__server_info[server_id]

    def shutdown_hana(self, ssh, os):
        self.__get_dao(os).shutdown_hana(ssh)


class HANAServerDBOperatorService:
    """ HANA Server DB operator, responsible for all DB relative operations, it's designed as singleton.
    To get the instance of this class: HANAServerDBOperatorService.instance()
    Initialize the class using HANAServerDBOperatorService() will raise an exception.
    """
    __instance = None

    @staticmethod
    def instance():
        """static access method for singleton"""
        if HANAServerDBOperatorService.__instance is None:
            HANAServerDBOperatorService()
        return HANAServerDBOperatorService.__instance

    def __init__(self):
        # implement the singleton class
        if HANAServerDBOperatorService.__instance is not None:
            raise MonitorDBOpError("This class is a singleton, use HANAServerDBOperatorService.instance() instead")
        else:
            HANAServerDBOperatorService.__instance = self
            self.__monitor_dao = HANAMonitorDAO(Mc.get_hana_server(),
                                                Mc.get_hana_port(),
                                                Mc.get_hana_user(),
                                                Mc.get_hana_password())
            self.__logger = Mu.get_logger(Mc.LOGGER_MONITOR_SERVER_DB_OPERATOR)

    def refresh_monitor_dao(self):
        # try to reconnect DB
        self.__monitor_dao.close_connection()
        self.__monitor_dao = HANAMonitorDAO(Mc.get_hana_server(),
                                            Mc.get_hana_port(),
                                            Mc.get_hana_user(),
                                            Mc.get_hana_password())

    # ----------------------------Below is for get information from DB--------------------------------------
    def get_email_admin(self, location_id):
        db_output = self.__monitor_dao.get_email_admin(location_id)
        try:
            administrators = [admin[0] for admin in db_output]
        except Exception as ex:
            administrators = []
            Mu.log_warning(self.__logger, "Parsing DB output failed in 'get_email_admin' "
                                          "with error: {0}, the output: {1}".format(ex, db_output))
        return administrators

    def get_locations(self):
        """get the location list"""
        db_output = self.__monitor_dao.get_locations()
        try:
            locations = [{Mc.FIELD_LOCATION_ID: location[0],
                          Mc.FIELD_LOCATION: location[1]} for location in db_output]
        except Exception as ex:
            locations = []
            Mu.log_warning(self.__logger, "Parsing DB output failed in 'get_locations' "
                                          "with error: {0}, the output: {1}".format(ex, db_output))
        return locations

    def get_server_full_names(self, location_id):
        """get the full host names (list) for all current servers by location"""
        db_output = self.__monitor_dao.get_server_full_names(location_id)
        try:
            server_full_names = [{Mc.FIELD_SERVER_ID: server[0],
                                  Mc.FIELD_SERVER_FULL_NAME: server[1],
                                  Mc.FIELD_MOUNT_POINT: server[2],
                                  Mc.FIELD_OS: server[3]} for server in db_output]
        except Exception as ex:
            server_full_names = []
            Mu.log_warning(self.__logger, "Parsing DB output failed in 'get_server_full_names' "
                                          "with error: {0}, the output: {1}".format(ex, db_output))
        return server_full_names

    def get_current_servers_info(self, location_id):
        """get the current (latest) server overview info"""
        db_output = self.__monitor_dao.get_current_servers_info(location_id)
        try:
            current_server_info = [{Mc.FIELD_SERVER_ID: server[0],
                                    Mc.FIELD_SERVER_FULL_NAME: server[1],
                                    Mc.FIELD_DISK_TOTAL: server[2],
                                    Mc.FIELD_DISK_FREE: server[3],
                                    Mc.FIELD_MEM_TOTAL: server[4],
                                    Mc.FIELD_MEM_FREE: server[5],
                                    Mc.FIELD_CPU_UTILIZATION: server[6],
                                    Mc.FIELD_OS: server[7],
                                    Mc.FIELD_CHECK_TIME: server[8],
                                    Mc.FIELD_CHECK_ID: server[9]} for server in db_output]
        except Exception as ex:
            current_server_info = []
            Mu.log_warning(self.__logger, "Parsing DB output failed in 'get_current_server_info' "
                                          "with error: {0}, the output: {1}".format(ex, db_output))
        return current_server_info

    def get_current_process_status(self, location_id):
        """get the latest processing status from monitor catalog """
        db_output = self.__monitor_dao.get_current_process_status(location_id)
        try:
            current_process_status = db_output[0][0]
        except Exception as ex:
            current_process_status = None
            Mu.log_warning(self.__logger, "Parsing DB output failed in 'get_current_process_status' "
                                          "with error: {0}, the output: {1}".format(ex, db_output))
        return current_process_status

    def get_top5_memory_consumers(self, server_id):
        db_output = self.__monitor_dao.get_top5_memory_consumers(server_id)
        try:
            top_5_consumers = [{Mc.FIELD_SERVER_FULL_NAME: consumer[0],
                                Mc.FIELD_USER_NAME: consumer[1],
                                Mc.FIELD_EMPLOYEE_NAME: consumer[2],
                                Mc.FIELD_EMAIL: consumer[3],
                                Mc.FIELD_USAGE: consumer[4],
                                Mc.FIELD_CHECK_ID: consumer[5]} for consumer in db_output]
        except Exception as ex:
            top_5_consumers = []
            Mu.log_warning(self.__logger, "Parsing DB output failed in 'get_top5_memory_consumers' "
                                          "with error: {0}, the output: {1}".format(ex, db_output))

        # sort with memory usage (desc) even the database view supposes to
        # sort it, to prevent some unaware changes of DB Views
        return sorted(top_5_consumers, key=itemgetter(Mc.FIELD_USAGE), reverse=True)

    def get_top5_disk_consumers(self, server_id):
        db_output = self.__monitor_dao.get_top5_disk_consumers(server_id)
        try:
            top_5_consumers = [{Mc.FIELD_SERVER_FULL_NAME: consumer[0],
                                Mc.FIELD_FOLDER: consumer[1],
                                Mc.FIELD_EMPLOYEE_NAME: consumer[2],
                                Mc.FIELD_EMAIL: consumer[3],
                                Mc.FIELD_USAGE: consumer[4],
                                Mc.FIELD_CHECK_ID: consumer[5]} for consumer in db_output]
        except Exception as ex:
            top_5_consumers = []
            Mu.log_warning(self.__logger, "Parsing DB output failed in 'get_top5_disk_consumers' "
                                          "with error: {0}, the output: {1}".format(ex, db_output))
        return top_5_consumers

    def get_top5_cpu_consumers(self, server_id):
        db_output = self.__monitor_dao.get_top5_cpu_consumers(server_id)
        try:
            top_5_consumers = [{Mc.FIELD_SERVER_FULL_NAME: consumer[0],
                                Mc.FIELD_USER_NAME: consumer[1],
                                Mc.FIELD_EMPLOYEE_NAME: consumer[2],
                                Mc.FIELD_EMAIL: consumer[3],
                                Mc.FIELD_USAGE: consumer[4],
                                Mc.FIELD_CHECK_ID: consumer[5]} for consumer in db_output]
        except Exception as ex:
            top_5_consumers = []
            Mu.log_warning(self.__logger, "Parsing DB output failed in 'get_top5_cpu_consumers' "
                                          "with error: {0}, the output: {1}".format(ex, db_output))
        return top_5_consumers

    def get_last3_servers_info(self, server_id, threshold, op_type):
        """get latest 3 server info by threshold for MEM, CPU or Disk"""
        db_output = self.__monitor_dao.get_last3_servers_info(server_id, threshold, op_type)
        try:
            last3_servers_info = [{Mc.FIELD_SERVER_FULL_NAME: server_info[0],
                                   Mc.FIELD_DISK_TOTAL: server_info[1],
                                   Mc.FIELD_DISK_FREE: server_info[2],
                                   Mc.FIELD_MEM_TOTAL: server_info[3],
                                   Mc.FIELD_MEM_FREE: server_info[4],
                                   Mc.FIELD_CPU_UTILIZATION: server_info[5],
                                   Mc.FIELD_CHECK_ID: server_info[6]} for server_info in db_output]
        except Exception as ex:
            last3_servers_info = []
            Mu.log_warning(self.__logger, "Parsing DB output failed in 'get_last3_servers_info' "
                                          "with error: {0}, the output: {1}".format(ex, db_output))

        return last3_servers_info

    def get_failed_servers(self, check_id, location_id):
        """only the server which the monitor processes failed on all the three stages will be selected"""
        db_output = self.__monitor_dao.get_failed_servers(check_id, location_id)
        try:
            failed_servers = [server[0] for server in db_output]
        except Exception as ex:
            failed_servers = []
            Mu.log_warning(self.__logger, "Parsing DB output failed in 'get_failed_servers' "
                                          "with error: {0}, the output: {1}".format(ex, db_output))
        return failed_servers

    def get_sid_mappings(self):
        db_output = self.__monitor_dao.get_sid_mappings()
        try:
            sid_mappings = [{Mc.FIELD_SID_START: mapping[0],
                             Mc.FIELD_SID_END: mapping[1],
                             Mc.FIELD_EMPLOYEE_ID: mapping[2]} for mapping in db_output]
        except Exception as ex:
            sid_mappings = []
            Mu.log_warning(self.__logger, "Parsing DB output failed in 'get_sid_mappings' "
                                          "with error: {0}, the output: {1}".format(ex, db_output))
        return sid_mappings

    def is_important_server(self, sid, server_id):
        db_output = self.__monitor_dao.get_flag_of_sid(sid, server_id)
        try:
            flag = True if db_output and db_output[0][0] == 'X' else False
        except Exception as ex:
            # prevent from shutting down the server when there is exception, set import flag to true
            flag = True
            Mu.log_warning(self.__logger, "Parsing DB output failed in 'is_important_server' "
                                          "with error: {0}, the output: {1}".format(ex, db_output))
        return flag

    def is_sudo_need_password(self, server_id):
        db_output = self.__monitor_dao.get_sudo_pwd_flag(server_id)
        try:
            sudo_pwd_flag = db_output[0][0] == 'X' if db_output else False
        except Exception as ex:
            sudo_pwd_flag = False
            Mu.log_warning(self.__logger, "Parsing DB output failed in 'is_sudo_need_password' "
                                          "with error: {0}, the output: {1}".format(ex, db_output))
        return sudo_pwd_flag

    def get_db_configuration(self, component, name):
        db_output = self.__monitor_dao.get_configuration(component, name)
        try:
            if name.endswith("_INT"):
                config_value = int(db_output[0][0]) if db_output else None
            else:
                config_value = db_output[0][0] if db_output else None
        except Exception as ex:
            config_value = None
            Mu.log_warning(self.__logger, "Parsing DB output failed in 'get_db_configuration' "
                                          "with error: {0}, the output: {1}".format(ex, db_output))
        return config_value

    # ----------------------------Below is for update/insert information to DB--------------------------------------
    def update_instance_info(self, check_id, server_id, info):
        """update instance info"""
        self.__monitor_dao.update_instance_info(check_id, server_id, info)

    def update_server_info(self, server_info, server_id):
        """update the server overview info"""
        self.__monitor_dao.update_server_info(server_info, server_id)

    def __update_server_monitoring_info(self, check_id, server_info, server_id):
        """update the server overview info to monitoring table"""
        self.__monitor_dao.update_server_monitoring_info(check_id, server_info, server_id)

    def update_cpu_monitoring_info(self, check_id, server_id, server_info, top_5_cpu_consumers):
        if server_info:
            self.__update_server_monitoring_info(check_id, server_info, server_id)
        if top_5_cpu_consumers:
            self.__monitor_dao.update_cpu_monitoring_info(check_id, server_id, top_5_cpu_consumers)

    def update_mem_monitoring_info(self, check_id, server_id, server_info, top_5_mem_consumers):
        if server_info:
            self.__update_server_monitoring_info(check_id, server_info, server_id)
        if top_5_mem_consumers:
            self.__monitor_dao.update_mem_monitoring_info(check_id, server_id, top_5_mem_consumers)

    def update_disk_monitoring_info(self, check_id, server_id, server_info, disk_consuming_info):
        if server_info:
            self.__update_server_monitoring_info(check_id, server_info, server_id)
        if disk_consuming_info:
            self.__monitor_dao.update_disk_monitoring_info(check_id, server_id, disk_consuming_info)

    def update_monitor_catalog(self, check_id, server_id, stage, status, location_id, message=None):
        self.__monitor_dao.update_monitor_catalog(check_id, server_id, stage, status, location_id, message)

    def update_monitor_catalog_overall_status(self, check_id, location_id):
        self.__monitor_dao.update_monitor_catalog_overall_status(check_id, location_id)

    def insert_sid_info(self, employee_id, server_name_list, sid_list):
        sid_info_list = []
        for server in server_name_list:
            # prepare the parameter list for the SQL:
            # insert into VAN_MONITOR.T_SID_INFO (SERVER_ID, SID, SID_USER, EMPLOYEE_ID) values (?,?,?)
            sid_info_list.extend(
                [(server[Mc.FIELD_SERVER_ID],
                  sid,
                  Mu.get_sidadm_from_sid(sid),
                  employee_id) for sid in sid_list])
        if sid_info_list:
            self.__monitor_dao.insert_sid_info(sid_info_list)

    def execute_from_script(self, sql_file):
        self.__monitor_dao.execute_from_script(sql_file)


class MonitorController(threading.Thread):
    """Monitor process controller, implementation of starting monitoring processes for
    all the servers by location via multiple threads.
    To create and start the thread, it needs to be initialized by following parameter:
    location_id: the location/group id of the servers
    """

    # define all monitors,
    # memory monitor has to be the first one,
    # disk monitor has to be the last one or the second last one if the last one is instance monitor.
    __monitors = [
        MonitorAdminExtension(MemoryMonitor()),
        MonitorAdminExtension(CPUMonitor()),
        MonitorAdminExtension(DiskMonitor()),
        InstanceInfoMonitor()]

    @staticmethod
    def get_locations():
        """get all locations from monitor
        return empty array if monitors is empty"""
        return MonitorController.__monitors[0].get_locations() if MonitorController.__monitors else []

    def __init__(self, location_id):
        """initialize the thread with location id"""
        threading.Thread.__init__(self, name="Thread-{0}".format(location_id))
        self.__location_id = location_id
        self.__check_interval = \
            MonitorController.__monitors[0].get_check_interval() if MonitorController.__monitors else None

    def run(self):
        """run the thread"""
        while True:
            monitor_check_id = datetime.now().strftime('%Y%m%d%H%M%S%f')
            for monitor in MonitorController.__monitors:
                monitor.monitoring(monitor_check_id, self.__location_id)

            # end loop if no monitor has been added
            if self.__check_interval is None:
                break
            time.sleep(self.__check_interval)


if __name__ == '__main__':

    for server_location in MonitorController.get_locations():
        MonitorController(server_location[Mc.FIELD_LOCATION_ID]).start()
