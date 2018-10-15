from util import MonitorUtility
import random


class LinuxMonitorOAOSimulator:
    @staticmethod
    def __get_sid_users(ssh):
        cmd_input, cmd_output, cmd_err = ssh.exec_command(
            "cut -d: -f1 /etc/passwd | grep ^[A-Za-z0-9][A-Za-z0-9][A-Za-z0-9][a]dm$", timeout=600)

        sid_users = [i.replace('\n', '') for i in cmd_output]
        return sid_users

    @staticmethod
    def simulate_get_disk_consumers(ssh, disk_total, disk_free):
        sid_users = LinuxMonitorOAOSimulator.__get_sid_users(ssh)
        disk_usage_overall = disk_total - disk_free
        disk_usage_info = []
        # for index, user in enumerate(sid_users):
        for user in sid_users:
            disk_usage = random.randint(0, disk_usage_overall)
            disk_usage_overall -= disk_usage
            disk_usage_info.append({'DISK_USAGE_KB': disk_usage, 'SID': MonitorUtility.get_sid_from_sidadm(user)})
        return disk_usage_info

    @staticmethod
    def simulate_collect_disk_info():
        return 3999312744, random.randint(1, int(3999312744/1.6))

    @staticmethod
    def simulate_collect_mem_info():
        mem_list = [264523944, 528041584, 1056518840]  # 256G, 512G, 1024G
        mem_total = mem_list[random.randint(0, 2)]
        mem_free = random.randint(int(mem_total/2), mem_total)
        return mem_total, mem_free

    @staticmethod
    def simulate_get_mem_consumers(ssh):
        sid_users = LinuxMonitorOAOSimulator.__get_sid_users(ssh)
        top_5_mem_consumer_info = []
        for user in sid_users:
            process_command = ['hdbindexserver', 'hdbnameserver', 'hdbxsengine', 'hdbwebdispatche', 'hdbpreprocessor']
            for i in range(0, 5):
                top_5_mem_consumer_info.append({'USER_NAME': user, 'PROCESS_COMMAND': process_command[i],
                                                'PROCESS_ID': i+10000, 'MEM': random.randint(1, 20)})
        return top_5_mem_consumer_info

    @staticmethod
    def simulate_collect_cpu_info():
        cpu_num_list = [80, 160, 176]
        cpu_num = cpu_num_list[random.randint(0, 2)]
        cpu_usage = random.randint(0, 100)
        return cpu_num, cpu_usage

    @staticmethod
    def simulate_get_cpu_consumers(ssh):
        sid_users = LinuxMonitorOAOSimulator.__get_sid_users(ssh)
        top_5_cpu_consumer_info = []
        for user in sid_users:
            process_command = ['hdbindexserver', 'hdbnameserver', 'hdbxsengine', 'hdbwebdispatche', 'hdbpreprocessor']
            for i in range(0, 5):
                top_5_cpu_consumer_info.append({'USER_NAME': user, 'PROCESS_COMMAND': process_command[i],
                                                'PROCESS_ID': i + 10000, 'CPU': random.randint(1, 5)})
        return top_5_cpu_consumer_info
