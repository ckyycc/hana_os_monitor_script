Python script part of HANA Server Operating System Monitoring Tool


Functionalities:

1.	monitors and saves all the resources (CPU, Disk and Memory) consumption information for all the configured servers;
2.	sends warning email to the top 5 resource consumers when the overall resource consumption of the server is passed the threshold;
3.  sends warning email to administrators if some server is not avaiable;
3.	Tries to shutdown HANA (via HDB stop) instance if the warning email has been sent for over three times.
