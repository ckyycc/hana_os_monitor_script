## Script (Python) Part of HANA Server Operating System Monitoring Tool


### Functionalities:

1.	Monitor and save all the resources (CPU, Disk and Memory) consumption information (to DB) for all the configured servers;
2.	Send warning email to the top 5 resource consumers when the threshold of resource consumption is exceeded;
3.  Send warning email to administrators if some server is not available;
4.	Try to shutdown HANA (via HDB stop) instance if the warning email has been sent for over three times (only valid for memory monitoring);
5.  Monitor and save the version info (to DB) for all hana instances.

### Design

#### Use Case Diagram

![class_diagram](https://raw.githubusercontent.com/ckyycc/hana_os_monitor_script/master/design/usecase.svg?sanitize=true)

#### Sequence Diagram

![sequence_diagram](https://raw.githubusercontent.com/ckyycc/hana_os_monitor_script/master/design/sequence.svg?sanitize=true)

#### Class Diagram

![class_diagram](https://raw.githubusercontent.com/ckyycc/hana_os_monitor_script/master/design/class.svg?sanitize=true)

