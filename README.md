# hadoop-tutorials

## Remote connect to hortonworks sandbox

In VirtualBox:
* Go to Settings -> Network, disable all current network adapters.
* Create a new Adapter, select `Host-only Adapter`, use the default settings and apply it.
* Start VM, ssh into it with the IP of your VM root@192.168.xxx.xx
* In the machine’s hosts, add
      192.168.xxx.xxx hortonworks.hbase.vm
      192.168.xxx.xxx sandbox.hortonworks.com
