# hadoop-tutorials

## Remte connect to hortonworks sandbox

In VirtualBox:
1. Go to Settings -> Network, disable all current network adapters.
2. Create a new Adapter, select `Host-only Adapter`, use the default settings and apply it.
3. Start VM, ssh into it with the IP of your VM root@192.168.xx.xx
4. In the machine’s hosts, add

192.168.56.101 hortonworks.hbase.vm
192.168.56.101 sandbox.hortonworks.com
